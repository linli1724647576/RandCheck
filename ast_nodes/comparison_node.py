# ComparisonNode类定义 - 比较表达式节点
from .ast_node import ASTNode
from .column_reference_node import ColumnReferenceNode
from .literal_node import LiteralNode
from .function_call_node import FunctionCallNode
from .arithmetic_node import ArithmeticNode
from data_structures.node_type import NodeType
from typing import Set, Tuple, List

class ComparisonNode(ASTNode):
    """比较表达式节点"""

    def __init__(self, operator: str):
        super().__init__(NodeType.COMPARISON)
        # 扩展操作符列表，包含更多SQL标准操作符
        self.supported_operators = {
            '=', '<>', '!=', '<', '>', '<=', '>=',
            'LIKE', 'NOT LIKE', 'RLIKE', 'REGEXP', 'NOT REGEXP',
            'IS NULL', 'IS NOT NULL', 'IN', 'NOT IN',
            'BETWEEN', 'NOT BETWEEN', 'EXISTS', 'NOT EXISTS'
        }
        
        # 验证操作符是否支持
        if operator not in self.supported_operators:
            raise ValueError(f"不支持的操作符: {operator}")
        
        self.operator = operator
        self.metadata = {
            'operator': operator,
            'is_aggregate': False  # 比较表达式不是聚合
        }

    def to_sql(self) -> str:
        if len(self.children) == 0:
            return ""

        left = self.children[0].to_sql() if self.children else ""

        # 单操作数操作符
        if self.operator in ['IS NULL', 'IS NOT NULL']:
            return f"{left} {self.operator}"
        
        # EXISTS/NOT EXISTS操作符
        if self.operator in ['EXISTS', 'NOT EXISTS']:
            if len(self.children) >= 1:
                subquery_sql = self.children[0].to_sql()
                # 确保子查询被括号包围
                if not (subquery_sql.startswith('(') and subquery_sql.endswith(')')):
                    subquery_sql = f"({subquery_sql})"
                return f"{self.operator} {subquery_sql}"
            return ""

        # 双操作数操作符
        if len(self.children) < 2:
            return f"{left} {self.operator}"
            
        right = self.children[1].to_sql()

        # IN/NOT IN操作符
        if self.operator in ['IN', 'NOT IN'] and right:
            # 检查右侧是否已经是子查询格式
            if not (right.startswith('(') and right.endswith(')')):
                # 如果不是子查询，则假设是列表
                return f"{left} {self.operator} ({right})"
            else:
                # 如果已经是子查询格式，则直接使用
                return f"{left} {self.operator} {right}"

        # BETWEEN/NOT BETWEEN操作符
        if self.operator in ['BETWEEN', 'NOT BETWEEN'] and len(self.children) >= 3:
            right1 = self.children[1].to_sql()
            right2 = self.children[2].to_sql()
            return f"{left} {self.operator} {right1} AND {right2}"

        # 其他双操作数操作符
        # 检查是否需要根据方言适配REGEXP操作符
        from data_structures.db_dialect import get_dialect_config
        dialect = get_dialect_config()
        operator = self.operator
        
        # PostgreSQL适配：将REGEXP、NOT REGEXP、RLIKE和NOT RLIKE转换为~和!~
        if dialect.name == 'POSTGRESQL':
            if operator == 'REGEXP' or operator == 'RLIKE':
                operator = '~'
            elif operator == 'NOT REGEXP' or operator == 'NOT RLIKE':
                operator = '!~'
        # PolarDB适配：PolarDB不支持RLIKE和NOT RLIKE操作符，使用LIKE替代
        elif dialect.name == 'POLARDB':
            if operator == 'RLIKE':
                operator = 'LIKE'
            elif operator == 'NOT RLIKE':
                operator = 'NOT LIKE'
        
        return f"({left} {operator} {right})"

    def collect_table_aliases(self) -> Set[str]:
        """收集比较表达式中引用的表别名"""
        aliases = set()
        for child in self.children:
            aliases.update(child.collect_table_aliases())
        return aliases

    def collect_column_aliases(self) -> Set[str]:
        """收集比较表达式中引用的列别名"""
        aliases = set()
        for child in self.children:
            aliases.update(child.collect_column_aliases())
        return aliases

    def _is_type_compatible(self, left_type, right_type):
        """检查两个数据类型是否兼容"""
        # 理想情况下，应该使用Column对象的category属性来判断类型兼容性
        # 但由于此方法设计为直接接收类型字符串而不是Column对象
        # 我们暂时保留基于数据类型字符串关键词的判断逻辑
        
        # 定义类型兼容性规则
        numeric_types = {'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'}
        string_types = {'VARCHAR', 'CHAR', 'TEXT', 'LONGTEXT', 'MEDIUMTEXT', 'TINYTEXT'}
        datetime_types = {'DATE', 'DATETIME', 'TIMESTAMP', 'TIME'}
        
        # 提取基本类型（去掉括号和长度信息）
        base_type1 = left_type.split('(')[0].upper() if left_type else 'UNKNOWN'
        base_type2 = right_type.split('(')[0].upper() if right_type else 'UNKNOWN'
        
        # 严格类型匹配：必须属于同一类型组
        if (base_type1 in numeric_types and base_type2 in numeric_types) or \
           (base_type1 in string_types and base_type2 in string_types) or \
           (base_type1 in datetime_types and base_type2 in datetime_types) or \
           base_type1 == base_type2:
            return True
        
        return False
        
    def _get_node_type(self, node):
        """获取节点的数据类型"""
        if isinstance(node, ColumnReferenceNode):
            return node.column.data_type
        elif isinstance(node, LiteralNode):
            return node.data_type
        elif isinstance(node, FunctionCallNode):
            return node.metadata.get('return_type', '')
        elif hasattr(node, 'metadata') and 'data_type' in node.metadata:
            return node.metadata['data_type']
        return ''
        
    def validate_columns(self, from_node: 'FromNode') -> Tuple[bool, List[str]]:
        """验证比较表达式中的列引用是否有效，包括类型兼容性检查"""
        errors = []
        
        # 首先验证所有子节点的列引用
        for child in self.children:
            if hasattr(child, 'validate_columns'):
                valid, child_errors = child.validate_columns(from_node)
                if not valid:
                    errors.extend(child_errors)
            elif isinstance(child, ColumnReferenceNode):
                if not child.is_valid(from_node):
                    errors.append(f"无效的列引用: {child.to_sql()}")
        
        # 对于二元比较符，检查左右两侧的类型兼容性
        if self.operator in ['=', '<>', '!=', '<', '>', '<=', '>=', 'LIKE', 'NOT LIKE', 'RLIKE', 'REGEXP', 'NOT REGEXP']:
            if len(self.children) >= 2:
                left_type = self._get_node_type(self.children[0])
                right_type = self._get_node_type(self.children[1])
                
                if left_type and right_type and not self._is_type_compatible(left_type, right_type):
                    errors.append(f"类型不匹配: {self.children[0].to_sql()} ({left_type}) 与 {self.children[1].to_sql()} ({right_type}) 在比较操作 {self.operator} 中")
        
        return (len(errors) == 0, errors)

    def repair_columns(self, from_node: 'FromNode') -> None:
        """修复比较表达式中的无效列引用和类型不兼容问题"""
        for i, child in enumerate(self.children):
            if hasattr(child, 'repair_columns'):
                child.repair_columns(from_node)
            elif isinstance(child, ColumnReferenceNode) and not child.is_valid(from_node):
                replacement = child.find_replacement(from_node)
                if replacement:
                    self.children[i] = replacement
        
        # 特殊处理BETWEEN操作符的类型兼容性问题
        if self.operator in ['BETWEEN', 'NOT BETWEEN'] and len(self.children) >= 3:
            # 获取左侧列的类型
            left_type = self._get_node_type(self.children[0])
            left_node = self.children[0]
            
            # 检查左侧是否有有效类型
            if left_type:
                base_left_type = left_type.split('(')[0].upper()
                
                # 处理两个边界值（BETWEEN value1 AND value2）
                for i in [1, 2]:
                    right_node = self.children[i]
                    right_type = self._get_node_type(right_node)
                    
                    # 检查右侧节点是否有有效类型，并且与左侧类型不兼容
                    if right_type and not self._is_type_compatible(left_type, right_type):
                        from data_structures.db_dialect import get_dialect_config
                        dialect = get_dialect_config()
                        
                        # 根据数据库方言进行类型转换
                        if dialect.name == 'POSTGRESQL':
                            from data_structures.function import Function
                            
                            # 处理日期时间类型转换
                            datetime_types = {'DATE', 'DATETIME', 'TIMESTAMP'}
                            numeric_types = {'INT', 'INTEGER', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'}
                            string_types = {'VARCHAR', 'TEXT', 'CHAR'}
                            
                            base_right_type = right_type.split('(')[0].upper()
                            
                            # 左侧是日期时间类型，右侧是数字类型
                            if base_left_type in datetime_types and base_right_type in numeric_types and isinstance(right_node, LiteralNode):
                                # 将整数字面量转换为日期相关表达式
                                # 创建 TO_DATE 函数调用节点
                                to_date_func = Function('TO_DATE', 'date', ['string', 'string'])
                                to_date_node = FunctionCallNode(to_date_func)
                                
                                # 添加 TO_DATE 的参数
                                to_date_node.add_child(LiteralNode('2023-01-01', 'DATE'))
                                to_date_node.add_child(LiteralNode('YYYY-MM-DD', 'STRING'))
                                
                                # 创建 + 算术表达式节点
                                plus_node = ArithmeticNode('+')
                                plus_node.add_child(to_date_node)
                                
                                # 创建 INTERVAL 表达式
                                interval_literal = LiteralNode(f'{right_node.value} days', 'STRING')
                                plus_node.add_child(interval_literal)
                                
                                # 替换原有的整数字面量
                                self.children[i] = plus_node
                            
                            # 数字类型与字符类型的转换
                            elif ((base_left_type in numeric_types and base_right_type in string_types) or 
                                  (base_right_type in numeric_types and base_left_type in string_types)) and isinstance(right_node, LiteralNode):
                                # 将字符类型转换为数字类型
                                cast_func = Function('CAST', 2, 2, ['any', 'string'], 'any', 'scalar')
                                cast_node = FunctionCallNode(cast_func)
                                cast_node.add_child(right_node)
                                # 添加目标数据类型参数
                                target_type = 'INTEGER' if base_left_type in numeric_types else 'STRING'
                                cast_node.add_child(LiteralNode(target_type, 'STRING'))
                                self.children[i] = cast_node
                        else:
                            # 对于MySQL、TIDB等其他方言，进行基本类型转换
                            # 尝试创建与左侧类型兼容的字面量
                            if isinstance(right_node, LiteralNode):
                                try:
                                    # 尝试根据左侧类型创建新的字面量
                                    if base_left_type == 'INT':
                                        new_value = int(right_node.value)
                                        self.children[i] = LiteralNode(new_value, 'INT')
                                    elif base_left_type in ['FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC']:
                                        new_value = float(right_node.value)
                                        self.children[i] = LiteralNode(new_value, 'FLOAT')
                                    elif base_left_type in ['VARCHAR', 'TEXT', 'CHAR']:
                                        new_value = str(right_node.value)
                                        self.children[i] = LiteralNode(new_value, 'VARCHAR')
                                    elif base_left_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                                        # 简单地尝试转换为日期字符串
                                        self.children[i] = LiteralNode('2023-01-01', base_left_type)
                                except:
                                    # 如果转换失败，使用一个安全的默认值
                                    if base_left_type in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC']:
                                        self.children[i] = LiteralNode(0, 'INT')
                                    elif base_left_type in ['VARCHAR', 'TEXT', 'CHAR']:
                                        self.children[i] = LiteralNode('default_value', 'VARCHAR')
                                    elif base_left_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                                        self.children[i] = LiteralNode('2023-01-01', base_left_type)

        # 处理常规二元比较操作符的类型兼容性问题
        if self.operator in ['=', '<>', '!=', '<', '>', '<=', '>='] and len(self.children) >= 2:
            left_type = self._get_node_type(self.children[0])
            right_type = self._get_node_type(self.children[1])
            
            # 检查左右两侧的类型是否兼容
            if left_type and right_type and not self._is_type_compatible(left_type, right_type):
                # 获取基础类型
                base_left_type = left_type.split('(')[0].upper() if left_type else ''
                base_right_type = right_type.split('(')[0].upper() if right_type else ''
                
                # 处理日期时间类型与数字类型的比较
                datetime_types = {'DATE', 'DATETIME', 'TIMESTAMP'}
                numeric_types = {'INT', 'INTEGER', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'}
                
                # 如果左侧是日期时间类型，右侧是数字类型
                if base_left_type in datetime_types and base_right_type in numeric_types and isinstance(self.children[1], LiteralNode):
                    # 右侧整数字面量转换为日期时间相关表达式
                    from data_structures.db_dialect import get_dialect_config
                    dialect = get_dialect_config()
                    
                    if dialect.name == 'POSTGRESQL':
                        from data_structures.function import Function
                        
                        # 创建 TO_DATE 函数调用节点
                        to_date_func = Function('TO_DATE', 'date', ['string', 'string'])
                        to_date_node = FunctionCallNode(to_date_func)
                        
                        # 添加 TO_DATE 的参数
                        to_date_node.add_child(LiteralNode('2023-01-01', 'DATE'))
                        to_date_node.add_child(LiteralNode('YYYY-MM-DD', 'STRING'))
                        
                        # 创建 + 算术表达式节点
                        plus_node = ArithmeticNode('+')
                        plus_node.add_child(to_date_node)
                        
                        # 创建 INTERVAL 表达式
                        interval_literal = LiteralNode(f'{self.children[1].value} days', 'STRING')
                        plus_node.add_child(interval_literal)
                        
                        # 替换原有的整数字面量
                        self.children[1] = plus_node
                
                # 如果右侧是日期时间类型，左侧是数字类型
                elif base_right_type in datetime_types and base_left_type in numeric_types and isinstance(self.children[0], LiteralNode):
                    # 左侧整数字面量转换为日期时间相关表达式
                    from data_structures.db_dialect import get_dialect_config
                    dialect = get_dialect_config()
                    
                    if dialect.name == 'POSTGRESQL':
                        from data_structures.function import Function
                        
                        # 创建 TO_DATE 函数调用节点
                        to_date_func = Function('TO_DATE', 'date', ['string', 'string'])
                        to_date_node = FunctionCallNode(to_date_func)
                        
                        # 添加 TO_DATE 的参数
                        to_date_node.add_child(LiteralNode('2023-01-01', 'DATE'))
                        to_date_node.add_child(LiteralNode('YYYY-MM-DD', 'STRING'))
                        
                        # 创建 + 算术表达式节点
                        plus_node = ArithmeticNode('+')
                        plus_node.add_child(to_date_node)
                        
                        # 创建 INTERVAL 表达式
                        interval_literal = LiteralNode(f'{self.children[0].value} days', 'STRING')
                        plus_node.add_child(interval_literal)
                        
                        # 替换原有的整数字面量
                        self.children[0] = plus_node
                
                # 处理整数类型与字符类型的比较
                elif (base_left_type in numeric_types and base_right_type in ['VARCHAR', 'TEXT', 'CHAR']) or \
                     (base_right_type in numeric_types and base_left_type in ['VARCHAR', 'TEXT', 'CHAR']):
                    # 检测整数类型与字符类型的比较
                    from data_structures.db_dialect import get_dialect_config
                    dialect = get_dialect_config()
                    
                    if dialect.name == 'POSTGRESQL':
                        # 在PostgreSQL中，添加显式类型转换
                        from data_structures.function import Function
                        
                        # 右侧是字符类型，左侧是数字类型
                        if base_left_type in numeric_types and base_right_type in ['VARCHAR', 'TEXT', 'CHAR']:
                            # 将字符串转换为数字类型
                            cast_func = Function('CAST', 2, 2, ['any', 'string'], 'any', 'scalar')
                            cast_node = FunctionCallNode(cast_func)
                            cast_node.add_child(self.children[1])
                            # 添加目标数据类型参数
                            cast_node.add_child(LiteralNode('INTEGER', 'STRING'))
                            self.children[1] = cast_node
                        # 左侧是字符类型，右侧是数字类型
                        elif base_right_type in numeric_types and base_left_type in ['VARCHAR', 'TEXT', 'CHAR']:
                            # 将数字转换为字符串类型
                            cast_func = Function('CAST', 2, 2, ['any', 'string'], 'any', 'scalar')
                            cast_node = FunctionCallNode(cast_func)
                            cast_node.add_child(self.children[0])
                            # 添加目标数据类型参数
                            cast_node.add_child(LiteralNode('STRING', 'STRING'))
                            self.children[0] = cast_node