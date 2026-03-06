import sqlglot
from sqlglot.expressions import TableAlias, table_
from generate_random_sql import get_tables

class ParameterTypeChecker:
    """
    参数类型检查工具类，用于检查SQL表达式中的参数类型
    基于表结构信息和列的类型属性进行判断
    """
    def __init__(self):
        """
        初始化参数类型检查器
        
        Args:
            tables: 表结构信息列表，如果未提供则从全局获取
        """
        self.tables = get_tables()
        print(f"初始化表结构: {self.tables}")
        self.alias_map = {}
    
    def build_alias_map(self, ast):
        """
        从AST中构建别名映射
        
        Args:
            ast: SQL解析树
        """
        if not ast:
            return
        
        # 清空之前的映射
        if self.alias_map:  
            self.alias_map.clear()
        print(self.alias_map)
        print(ast)
        self._collect_aliases(ast)
    
    def _collect_aliases(self, node):
        """
        递归收集别名信息
        
        Args:
            node: AST节点
        """
        if not node:
            return
        
        # 检查是否是别名节点
        # 处理列别名或表达式别名
        if isinstance(node , sqlglot.expressions.Alias):
            if hasattr(node, 'this') and hasattr(node, 'alias'):
                # 获取原始表达式的字符串表示
                original_expr = str(node.this)
                alias = getattr(node, 'alias').lower()
                # 将别名（小写）映射到原始表达式字符串
                self.alias_map[alias] = original_expr
        
        # 处理表别名
        elif isinstance(node, sqlglot.expressions.Table):
            if hasattr(node,'alias') and hasattr(node,'this'):
                alias=str(node.alias)
                original_expr=str(node.this)
                self.alias_map[alias] = original_expr
            
            
        
        # 递归处理子节点
        if hasattr(node, 'args'):
            for child in node.args.values():
                if isinstance(child, (list, tuple)):
                    for item in child:
                        self._collect_aliases(item)
                else:
                    self._collect_aliases(child)
    
    def get_column_info(self, column_name):
        """
        获取列的类型信息
        
        Args:
            column_name: 列名（可能包含表名或别名）
            
        Returns:
            dict: 包含列名、类型、是否为空和是否为数值类型的字典
        """
        print(self.alias_map)
        # 将列名转换为小写以进行不区分大小写的比较
        column_name_lower = column_name.lower()
        
        # 检查并移除distinct关键字
        if 'distinct' in column_name_lower:
            column_name_lower = column_name_lower.replace('distinct', '').strip()
        
        # 处理别名
        if '.' in column_name_lower:
            table_part, column_part = column_name_lower.split('.', 1)
            # 检查表别名
            if table_part in self.alias_map:
                actual_table = self.alias_map[table_part]
                column_to_check = f"{actual_table}.{column_part}"
            else:
                column_to_check = column_name_lower
        else:
            # 检查完整列名别名
            if column_name_lower in self.alias_map:
                column_to_check = self.alias_map[column_name_lower]
            else:
                column_to_check = column_name_lower
        
        # 初始化返回结果
        column_info = {
            'name': column_to_check,
            'type': None,
            'has_nulls': False,
            'is_numeric': False
        }
        
        # 从表结构中查找列信息
        self.tables = get_tables()

        if self.tables:

            found = False
            target_table = None
            target_column = column_to_check
            print(f"    检查列: {column_to_check}")
            # 处理表名.列名形式
            if '.' in column_to_check:
                table_part, column_part = column_to_check.split('.', 1)
                target_table = table_part
                target_column = column_part
            
            # 优先在指定表中查找
            if target_table:
                print(f"    目标表: {target_table}")
                for table in self.tables:
                    if table.name.lower() == target_table:
                        print(f"    目标列: {target_column}")
                        for column in table.columns:
                            if column.name == target_column:
                                column_info['type'] = column.data_type
                                column_info['has_nulls'] = column.is_nullable
                                column_info['is_numeric'] = column.category == 'numeric'
                                found = True
                                break
                        if found:
                            break
            
            # 在所有表中查找
            if not found:
                for table in self.tables:
                    for column in table.columns:
                        if column.name.lower() == column_to_check or (target_column and column.name.lower() == target_column):
                            column_info['type'] = column.data_type
                            column_info['has_nulls'] = column.is_nullable
                            column_info['is_numeric'] = column.category == 'numeric'
                            found = True
                            break
                    if found:
                        break
        
        return column_info
    
    def is_numeric_column(self, column_name):
        """
        检查列是否为数值类型，支持带表名的列引用（如t2.c4）
        
        Args:
            column_name: 列名（可能包含表名或别名）
            
        Returns:
            bool: 如果列是数值类型则返回True，否则返回False
        """
        print(f"    检查列名: {column_name}")
        column_info = self.get_column_info(column_name)
        print(f"    列信息: {column_info}")
        return column_info['is_numeric']
    
    def is_date_column(self, column_name):
        """
        检查列是否为日期类型
        
        Args:
            column_name: 列名（可能包含表名或别名）
            
        Returns:
            bool: 如果列是日期类型则返回True，否则返回False
        """
        column_info = self.get_column_info(column_name)
        if not column_info['type']:
            return False
        
        type_lower = column_info['type'].lower()
        return 'date' in type_lower and 'time' not in type_lower
    
    def is_datetime_column(self, column_name):
        """
        检查列是否为日期时间类型
        
        Args:
            column_name: 列名（可能包含表名或别名）
            
        Returns:
            bool: 如果列是日期时间类型则返回True，否则返回False
        """
        column_info = self.get_column_info(column_name)
        if not column_info['type']:
            return False
        
        type_lower = column_info['type'].lower()
        return ('datetime' in type_lower or 
                'timestamp' in type_lower or 
                ('date' in type_lower and 'time' in type_lower))
    
    def is_string_column(self, column_name):
        """
        检查列是否为字符串类型
        
        Args:
            column_name: 列名（可能包含表名或别名）
            
        Returns:
            bool: 如果列是字符串类型则返回True，否则返回False
        """
        column_info = self.get_column_info(column_name)
        
        # 如果已知是数值类型，则不是字符串类型
        if column_info.get('is_numeric') is not None:
            return not column_info['is_numeric']
        
        # 根据类型名称判断
        if not column_info['type']:
            return False
        
        type_lower = column_info['type'].lower()
        return any(keyword in type_lower for keyword in ['char', 'text', 'string'])
    
    def _get_function_return_type(self, func_node):
        """
        获取函数调用的返回类型
        
        Args:
            func_node: 函数节点
            
        Returns:
            str: 函数返回类型，可以是 'numeric', 'string', 'datetime', 'date', 'any'
        """
        if not func_node:
            return 'any'
        
        func_class = func_node.__class__.__name__
        
        # 直接使用类名作为函数名
        func_name = func_class.upper()
        
        # 数值函数列表
        numeric_functions = ['ABS', 'SQRT', 'POW', 'EXP', 'LN', 'LOG', 'LOG10', 'LOG2',
                           'SIN', 'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN', 'COT',
                           'ROUND', 'TRUNCATE', 'CEIL', 'CEILING', 'FLOOR',
                           'SUM', 'AVG', 'MAX', 'MIN', 'COUNT', 'VARIANCE', 'STDDEV',
                           'MEDIAN', 'SUM_DISTINCT', 'COUNT_DISTINCT']
        
        # 字符串函数列表
        string_functions = ['LOWER', 'UPPER', 'CONCAT', 'LTRIM', 'RTRIM', 'TRIM',
                          'REPLACE', 'REPEAT', 'LPAD', 'RPAD', 'REVERSE', 'LENGTH',
                          'SUBSTRING', 'CHAR', 'ASCII', 'CONCAT_WS', 'FIND_IN_SET',
                          'RIGHT','LEFT']
        
        # 日期函数列表
        date_functions = ['DATE', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
                        'DATEDIFF', 'TIMEDIFF', 'DATE_ADD', 'DATE_SUB', 'DATE_FORMAT',
                        'NOW', 'CURDATE', 'CURTIME', 'UNIX_TIMESTAMP', 'FROM_UNIXTIME']
        
        # 日期时间函数列表
        datetime_functions = ['DATETIME', 'TIMESTAMP', 'FROM_UNIXTIME', 'UNIX_TIMESTAMP',
                            'NOW', 'SYSDATE', 'CURRENT_TIMESTAMP']
        
        if func_name in numeric_functions:
            return 'numeric'
        elif func_name in string_functions:
            return 'string'
        elif func_name in datetime_functions:
            return 'datetime'
        elif func_name in date_functions:
            return 'date'
        
        # 默认返回any类型
        return 'any'
    
    def is_numeric_parameter(self, param_node):
        """
        检查参数节点是否为数值类型
        
        Args:
            param_node: 要检查的参数节点
            
        Returns:
            bool: 如果参数是数值类型则返回True，否则返回False
        """
        if not param_node:
            return False
        
        print(f"  is_numeric_parameter调用，参数节点类型: {type(param_node).__name__}")
        
        param_class = param_node.__class__.__name__
        
        # 处理列引用
        if param_class in ['Column', 'Ref', 'Identifier']:
            print(f"    参数类是Column/Ref，处理列引用")
            # 获取完整的表名和列名
            full_column_name = str(param_node)
            print(f"    完整列引用: {full_column_name}")
            
            # 检查列是否为数值类型
            result = self.is_numeric_column(full_column_name)
            print(f"    is_numeric_column返回: {result}")
            return result
        
        # 处理函数调用 - 检查所有可能的函数节点
        # 根据测试，sqlglot中的函数节点类名是函数名本身（如Abs、Sum等）
        # 因此我们不需要检查是否以"Func"结尾
        func_name = param_class.upper()
        
        # 检查是否为已知函数
        all_functions = ['ABS', 'SQRT', 'POW', 'EXP', 'LN', 'LOG', 'LOG10', 'LOG2',
                        'SIN', 'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN', 'COT',
                        'ROUND', 'TRUNCATE', 'CEIL', 'CEILING', 'FLOOR',
                        'SUM', 'AVG', 'MAX', 'MIN', 'COUNT', 'VARIANCE', 'STDDEV',
                        'MEDIAN', 'SUM_DISTINCT', 'COUNT_DISTINCT',
                        'LOWER', 'UPPER', 'CONCAT', 'LTRIM', 'RTRIM', 'TRIM',
                        'REPLACE', 'REPEAT', 'LPAD', 'RPAD', 'REVERSE', 'LENGTH',
                        'SUBSTRING', 'CHAR', 'ASCII', 'CONCAT_WS', 'FIND_IN_SET',
                        'DATE', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
                        'DATEDIFF', 'TIMEDIFF', 'DATE_ADD', 'DATE_SUB', 'DATE_FORMAT',
                        'NOW', 'CURDATE', 'CURTIME', 'UNIX_TIMESTAMP', 'FROM_UNIXTIME',
                        'DATETIME', 'TIMESTAMP', 'FROM_UNIXTIME', 'UNIX_TIMESTAMP',
                        'NOW', 'SYSDATE', 'CURRENT_TIMESTAMP']
        
        if func_name in all_functions:
            # 优先使用内部函数的返回类型
            return_type = self._get_function_return_type(param_node)
            
            if return_type == 'numeric':
                return True
            elif return_type == 'string' or return_type == 'date' or return_type == 'datetime':
                return False
            elif return_type == 'any':
                # 如果返回类型为any，则检查函数参数类型
                if hasattr(param_node, 'args') and param_node.args:
                    # 处理第一个参数
                    first_arg_key = list(param_node.args.keys())[0]
                    first_arg = param_node.args[first_arg_key]
                    return self.is_numeric_parameter(first_arg)
        
        # 处理负号节点
        if param_class == 'Neg':
            if hasattr(param_node, 'this'):
                return self.is_numeric_parameter(param_node.this)
        
        # 处理数值字面量
        if param_class in ['Number', 'Float', 'Integer']:
            return True
        # sqlglot 常用 Literal 表示字面量（可能是字符串或数值）
        if param_class == 'Literal':
            if getattr(param_node, 'is_string', False):
                return False
            # 尝试解析为数值
            try:
                float(param_node.this)
                return True
            except (TypeError, ValueError):
                return False
        
        # 处理其他表达式类型
        return False
    
    def is_string_parameter(self, param_node):
        """
        检查参数节点是否为字符串类型
        
        Args:
            param_node: 要检查的参数节点
            
        Returns:
            bool: 如果参数是字符串类型则返回True，否则返回False
        """
        if not param_node:
            return False
        
        param_class = param_node.__class__.__name__
        
        # 处理列引用
        if param_class in ['Column', 'Ref', 'Identifier']:
            # 获取完整的表名和列名
            full_column_name = str(param_node)
            
            # 检查列是否为字符串类型
            return self.is_string_column(full_column_name)
        
        # 处理函数调用 - 检查所有可能的函数节点
        # 根据测试，sqlglot中的函数节点类名是函数名本身（如Abs、Sum等）
        if param_class == 'Anonymous':
            param_class = param_node.this
        func_name = param_class.upper()
        
        # 检查是否为已知函数
        all_functions = ['ABS', 'SQRT', 'POW', 'EXP', 'LN', 'LOG', 'LOG10', 'LOG2',
                        'SIN', 'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN', 'COT',
                        'ROUND', 'TRUNCATE', 'CEIL', 'CEILING', 'FLOOR',
                        'SUM', 'AVG', 'MAX', 'MIN', 'COUNT', 'VARIANCE', 'STDDEV',
                        'MEDIAN', 'SUM_DISTINCT', 'COUNT_DISTINCT',
                        'LOWER', 'UPPER', 'CONCAT', 'LTRIM', 'RTRIM', 'TRIM',
                        'REPLACE', 'REPEAT', 'LPAD', 'RPAD', 'REVERSE', 'LENGTH',
                        'SUBSTRING', 'CHAR', 'ASCII', 'CONCAT_WS', 'FIND_IN_SET',
                        'DATE', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
                        'DATEDIFF', 'TIMEDIFF', 'DATE_ADD', 'DATE_SUB', 'DATE_FORMAT',
                        'NOW', 'CURDATE', 'CURTIME', 'UNIX_TIMESTAMP', 'FROM_UNIXTIME',
                        'DATETIME', 'TIMESTAMP', 'FROM_UNIXTIME', 'UNIX_TIMESTAMP',
                        'NOW', 'SYSDATE', 'CURRENT_TIMESTAMP']
        
        if func_name in all_functions:
            # 优先使用内部函数的返回类型
            return_type = self._get_function_return_type(param_node)
            
            if return_type == 'string':
                return True
            elif return_type == 'numeric' or return_type == 'date' or return_type == 'datetime':
                return False
            elif return_type == 'any':
                # 如果返回类型为any，则检查函数参数类型
                if hasattr(param_node, 'args') and param_node.args:
                    # 处理第一个参数
                    first_arg_key = list(param_node.args.keys())[0]
                    first_arg = param_node.args[first_arg_key]
                    return self.is_string_parameter(first_arg)
        
        # 处理字符串字面量
        if param_class in ['Literal', 'String']:
            return True
        
        # 处理其他表达式类型
        return False
    
    def is_datetime_parameter(self, param_node):
        """
        检查参数节点是否为日期时间类型

        Args:
            param_node: 要检查的参数节点
            
        Returns:
            bool: 如果参数是日期时间类型则返回True，否则返回False
        """
        if not param_node:
            return False
        
        param_class = param_node.__class__.__name__
        
        # 处理列引用
        if param_class in ['Column', 'Ref', 'Identifier']:
            # 获取完整的表名和列名
            full_column_name = str(param_node)
            
            # 获取列信息
            column_info = self.get_column_info(full_column_name)
            
            # 如果已知是数值类型，则不是日期时间类型
            if column_info.get('is_numeric') is True:
                return False
            
            # 根据类型名称判断
            if column_info['type']:
                type_lower = column_info['type'].lower()
                return any(keyword in type_lower for keyword in ['date', 'time', 'datetime', 'timestamp'])
            else:
                # 如果没有类型信息（如测试环境），对于时间函数的参数，默认假设是日期时间类型
                return True
        
        # 处理函数调用 - 检查所有可能的函数节点
        # 根据测试，sqlglot中的函数节点类名是函数名本身（如Abs、Sum等）
        func_name = param_class.upper()
        
        # 检查是否为已知函数
        all_functions = ['ABS', 'SQRT', 'POW', 'EXP', 'LN', 'LOG', 'LOG10', 'LOG2',
                        'SIN', 'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN', 'COT',
                        'ROUND', 'TRUNCATE', 'CEIL', 'CEILING', 'FLOOR',
                        'SUM', 'AVG', 'MAX', 'MIN', 'COUNT', 'VARIANCE', 'STDDEV',
                        'MEDIAN', 'SUM_DISTINCT', 'COUNT_DISTINCT',
                        'LOWER', 'UPPER', 'CONCAT', 'LTRIM', 'RTRIM', 'TRIM',
                        'REPLACE', 'REPEAT', 'LPAD', 'RPAD', 'REVERSE', 'LENGTH',
                        'SUBSTRING', 'CHAR', 'ASCII', 'CONCAT_WS', 'FIND_IN_SET',
                        'DATE', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
                        'DATEDIFF', 'TIMEDIFF', 'DATE_ADD', 'DATE_SUB', 'DATE_FORMAT',
                        'NOW', 'CURDATE', 'CURTIME', 'UNIX_TIMESTAMP', 'FROM_UNIXTIME',
                        'DATETIME', 'TIMESTAMP', 'FROM_UNIXTIME', 'UNIX_TIMESTAMP',
                        'NOW', 'SYSDATE', 'CURRENT_TIMESTAMP']
        
        if func_name in all_functions:
            # 优先使用内部函数的返回类型
            return_type = self._get_function_return_type(param_node)
            
            if return_type == 'date' or return_type == 'datetime':
                return True
            elif return_type == 'numeric' or return_type == 'string':
                return False
            elif return_type == 'any':
                # 如果返回类型为any，则检查函数参数类型
                if hasattr(param_node, 'args') and param_node.args:
                    # 处理第一个参数
                    first_arg_key = list(param_node.args.keys())[0]
                    first_arg = param_node.args[first_arg_key]
                    return self.is_datetime_parameter(first_arg)
        
        # 处理日期时间字面量
        if param_class in ['Literal', 'String']:
            # 简单检查字符串是否看起来像日期时间
            if hasattr(param_node, 'this'):
                value = str(param_node.this)
                result = any(keyword in value for keyword in ['-', ':', ' '])
                return result
        
        # 处理其他表达式类型
        return False

# 全局参数类型检查器实例
global_parameter_checker = ParameterTypeChecker()
