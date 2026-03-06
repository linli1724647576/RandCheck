import copy
import json
import math
import re
import sys
from nt import kill
import sqlglot
import os
import random
from get_seedQuery import SeedQueryGenerator
from generate_random_sql import get_tables, create_sample_functions
from data_structures.db_dialect import DBDialectFactory
from data_structures.function import Function
# 导入detail_mutator中的变异器工厂

class AggregateMathEquivalenceMutator:
    """用于实现聚合函数数学性质等价变换的类"""
    def __init__(self, ast, db_config=None):
        self.ast = ast
        self._normalize_joins_to_inner()
        self.aggregate_nodes = []  # 存储识别到的聚合函数节点
        self.mutated = False  # 标记是否已变异
        self.mutations_applied = False  # 标记是否应用了变异
        self.is_sum_to_avg_count_mutation = False  # 标记是否是SUM→AVG*COUNT变异
        self.is_parameter_equivalence_mutation = False  # 标记是否是参数等价变换变异
        self.tables = get_tables()  # 导入全局表结构信息
        self.alias_map = {}  # 存储别名到实际引用的映射关系
        self.db_config = db_config
        # 获取当前数据库方言
        self.current_dialect = DBDialectFactory._current_dialect
        self.is_mariadb = self.current_dialect and self.current_dialect.name == "MARIADB"
        # 初始化函数列表
        self.functions = create_sample_functions()
        self.function_map = {func.name.lower(): func for func in self.functions}
        self._scalar_function_keys = {
            self._normalize_function_key(func.name)
            for func in self.functions
            if func.func_type == 'scalar'
        }
        # 初始化变异器工厂
        self._geometry_compare_cache = {}
        self._geometry_compare_connection = None
        # 定义聚合函数和三角函数列表
        self._aggregate_functions = [
            # 聚合函数
            'Avg', 'Count', 'Max', 'Min', 'Sum',
            'GroupConcat',
            'Std', 'Stddev', 'StddevPop', 'StddevSamp',
            'Variance', 'VariancePop', 'VarPop', 'VarSamp', 'StdDevPop', 'StdDevSamp',
            'BitAnd', 'BitOr', 'BitXor', 'JSON_ArrayAgg', 'JSON_GroupConcat',
            'ST_Collect'
        ]
        self._trigonometric_functions = [
            # 三角函数
            'Sin', 'Cos', 'Tan', 'Asin', 'Acos', 'Atan', 'Atan2',
            'Sinh', 'Cosh', 'Tanh', 'Asinh', 'Acosh', 'Atanh'
        ]
        self._scalar_functions = ['Floor','Ceil','Reverse','SHA2','SHA1','Length','Char',
                                       'ASCII','Year','Month','Day','Hour','Minute','Second','Sub',
                                       'Power','Pow','Date','Length','Lower','Ln','Sqrt','Exp','Round',
                                       'Right','Left','ConcatWs','SubstringIndex','Replace','Trim',
                                       'Repeat','Atan2','Cos','Sin','Tan','Cot','Log','Log10','Atan1','Atan','Acos',
                                       'Asin','SHA','SHA2','MD5','JSON_Extract','JSON_Insert','JSON_Replace','JSONSet',
                                       'Abs','StrtoDate','Date_format','Nullif','Upper','Point','TimeDiff',
                                       'TsOrDsToDate']
        # 构建别名映射
        self._build_alias_map(ast)

    def _normalize_joins_to_inner(self):
        """Preprocess: force all join types to INNER to avoid outer-join semantics changes during mutation."""
        if not self.ast:
            return
        try:
            import sqlglot
            from sqlglot import expressions as exp
        except Exception:
            return
        for join in self.ast.find_all(exp.Join):
            # sqlglot uses kind arg for join type; None/'' means INNER
            join.set("kind", None)

    def _get_sqlglot_dialect_name(self) -> str:
        """Map current dialect to sqlglot dialect name."""
        if self.current_dialect and self.current_dialect.name.upper() == "POSTGRESQL":
            return "postgres"
        return "mysql"

    def find_aggregate_nodes(self, ast):
        """识别AST中所有的聚合函数节点"""
        if not ast:
            return

        # 清空之前的结果
        self.aggregate_nodes = []
        print(f"开始查找聚合函数节点")
        self._find_aggregate_nodes(ast)
        return self.aggregate_nodes

    def _build_alias_map(self, ast):
        """构建别名到实际列的映射关系"""
        if not ast:
            return
        
        # 遍历AST收集别名信息
        self._collect_aliases(ast)
    
    def _collect_aliases(self, node):
        """递归收集别名信息"""
        if not node:
            return
        
        # 检查是否是别名节点
        if hasattr(node, 'alias') and getattr(node, 'alias', None):
            if hasattr(node, 'this'):
                # 获取原始表达式的字符串表示
                original_expr = str(node.this)
                alias = getattr(node, 'alias').lower()
                # 将别名（小写）映射到原始表达式
                self.alias_map[alias] = original_expr
                # 打印别名映射构建信息

        
        # 递归处理子节点
        if hasattr(node, 'args'):
            for child in node.args.values():
                if isinstance(child, (list, tuple)):
                    for item in child:
                        if hasattr(item, 'args'):
                            self._collect_aliases(item)
                elif hasattr(child, 'args'):
                    self._collect_aliases(child)
    
    def _find_aggregate_nodes(self, ast, parent_node=None):
        """递归查找聚合函数节点"""
        if not ast:
            return
        
        # 检查当前节点是否是函数节点
        if self._is_function_node(ast):
            # 记录函数节点信息
            node_info = self._add_aggregate_node(ast)
            
            # 确定节点所在的子句类型
            clause_type = self._determine_clause_type(ast)
            node_info['clause_type'] = clause_type
            
            # 为节点生成字符串表示，用于识别相同表达式的其他节点
            node_info['expr_str'] = str(ast)
            
            # 记录父节点信息
            node_info['parent_node'] = parent_node
        
        # 递归处理子节点
        if hasattr(ast, 'args'):
            #print(f"当前节点: {ast}")
            #print(f"当前节点的类型: {ast.__class__.__name__}")
            for child in ast.args.values():
                if isinstance(child, (list, tuple)):
                    for item in child:
                        self._find_aggregate_nodes(item, ast)
                else:
                    self._find_aggregate_nodes(child, ast)

    def _determine_clause_type(self, node):
        """确定节点所在的子句类型"""
        current = node
        while hasattr(current, 'parent') and current.parent:
            current = current.parent
            #print(f"当前节点的父节点: {current}")
            #print(f"当前节点的父节点的类型: {current.__class__.__name__}")

            if current.__class__.__name__ == 'Order':
                return 'ORDER BY'
            elif current.__class__.__name__ == 'Select':
                # 检查是否是SELECT子句的一部分
                return 'SELECT'
            elif current.__class__.__name__ == 'Group':
                return 'GROUP BY'
            elif current.__class__.__name__ == 'Where':
                return 'WHERE'
            elif current.__class__.__name__ == 'Having':
                return 'HAVING'
        return 'OTHER'

    def _is_function_node(self, node):
        """
        检查节点是否为任何函数节点（聚合、窗口、标量或三角函数）
        
        Args:
            node: SQLGlot AST节点
            
        Returns:
            bool: 如果节点是任何类型的函数节点则返回True，否则返回False
        """
        if not node:
            return False
        
        # 检查是否为聚合函数
        if self._is_aggregate_function(node):
            return True
            
        # 检查是否为窗口函数
        if self._is_window_function(node):
            return True
            
        # 检查是否为标量函数
        if self._is_scalar_function(node):
            return True
            
        # 检查是否为Anonymous类型函数
        if node.__class__.__name__ == 'Anonymous':
            return True
            
        return False
    
    def _is_aggregate_function(self, node):
        """
        检查节点是否为聚合函数节点
        
        Args:
            node: SQLGlot AST节点
            
        Returns:
            bool: 如果节点是聚合函数节点则返回True，否则返回False
        """
        if not node:
            return False
        
        # 获取节点的类名
        node_class = node.__class__.__name__
        
        # 检查是否是预定义的聚合函数
        if node_class in self._aggregate_functions:
            return True
            
        # 处理Anonymous类型的函数（如VARIANCE, STDDEV等）
        if node_class == 'Anonymous':
            if hasattr(node, 'this'):
                this_value = node.this
                if isinstance(this_value, str):
                    return this_value.lower() in [f.lower() for f in self._aggregate_functions]
                return this_value in self._aggregate_functions
            elif hasattr(node, 'name'):
                name_value = node.name
                if isinstance(name_value, str):
                    return name_value.lower() in [f.lower() for f in self._aggregate_functions]
                return name_value in self._aggregate_functions
        
        # 检查是否是sqlglot的聚合函数类
        if node_class.endswith('Func'):
            # 从function_map中获取函数信息
            func_name = node_class.lower()
            if func_name in self.function_map:
                return self.function_map[func_name].func_type == 'aggregate'
        
        return False
        
    def _is_window_function(self, node):
        """
        检查节点是否为窗口函数节点
        
        Args:
            node: SQLGlot AST节点
            
        Returns:
            bool: 如果节点是窗口函数节点则返回True，否则返回False
        """
        if not node:
            return False
            
        # 检查节点是否为Window类（sqlglot中窗口函数的包装类）
        node_class = node.__class__.__name__
        if node_class == 'Window':
            return True
        
        # 检查节点是否包含OVER子句
        if hasattr(node, 'over') and node.over:
            return True
            
        # 从function_map中获取函数信息
        if node_class.endswith('Func'):
            func_name = node_class.lower()
            if func_name in self.function_map:
                return self.function_map[func_name].func_type == 'window'
        
        # 处理Anonymous类型的函数
        if node_class == 'Anonymous':
            if hasattr(node, 'this'):
                this_value = node.this
                if isinstance(this_value, str):
                    func_name = this_value.lower()
                    if func_name in self.function_map:
                        return self.function_map[func_name].func_type == 'window'
            elif hasattr(node, 'name'):
                name_value = node.name
                if isinstance(name_value, str):
                    func_name = name_value.lower()
                    if func_name in self.function_map:
                        return self.function_map[func_name].func_type == 'window'
        
        return False
        
    def _is_scalar_function(self, node):
        """
        检查节点是否为标量函数节点（非聚合、非窗口函数）
        
        Args:
            node: SQLGlot AST节点
            
        Returns:
            bool: 如果节点是标量函数节点则返回True，否则返回False
        """
        #print(f"检查节点类型: {node.__class__.__name__}")
        if node.__class__.__name__ == 'Anonymous':
            name_value = node.name if hasattr(node, 'name') else node.this
            if isinstance(name_value, str):
                name_upper = name_value.upper()
                if name_upper.startswith('ST_'):
                    return name_upper not in {f.upper() for f in self._aggregate_functions}
                normalized_name = self._normalize_function_key(name_value)
                if normalized_name in self._scalar_function_keys:
                    return True
            print(f"node.__class__.__name__={node.name}")
            return node.name.lower() in [f.lower() for f in self._scalar_functions]

        normalized_class = self._normalize_function_key(node.__class__.__name__)
        if normalized_class in self._scalar_function_keys:
            return True
        if node.__class__.__name__.lower() in [f.lower() for f in self._scalar_functions]:
            return True
        return False

    def _normalize_function_key(self, name):
        if not isinstance(name, str):
            return ''
        return name.replace('_', '').lower()

    def _get_outer_function_node(self, node):
        """
        获取嵌套函数中的外部函数节点
        
        Args:
            node: 内部函数节点
            
        Returns:
            tuple: (outer_node, is_nested) - 外部函数节点和是否为嵌套结构
        """
        if not node.parent:
            return None, False
            
        current_node = node
        parent_node = node.parent
        outer_function_node = None
        
        # 遍历父节点链，寻找最外层的函数节点
        while parent_node:
            # 检查当前父节点是否为Window类节点（窗口函数）
            if parent_node.__class__.__name__ == 'Window':
                outer_function_node = parent_node
                break
            
            # 检查当前父节点是否为Ordered或Order类型（ORDER BY子句中的表达式或子句）
            # 如果是，继续向上遍历，因为它可能是窗口函数的一部分
            if parent_node.__class__.__name__ in ['Ordered', 'Order']:
                current_node = parent_node
                parent_node = getattr(parent_node, 'parent', None)
                continue
            if parent_node.__class__.__name__ == 'Alias':
                outer_function_node = current_node
                break
            # 检查当前父节点是否为函数节点（聚合或标量函数）
            is_func = (hasattr(current_node, 'args') and 
                      (self._is_aggregate_function(current_node) or 
                       self._is_scalar_function(current_node)))
            print(f"当前节点：{current_node.__class__.__name__}")
            print(f"当前父节点: {parent_node.__class__.__name__}")
            print(f"当前父节点的父节点: {parent_node.parent.__class__.__name__}")
            if (is_func or current_node.__class__.__name__ in ['Add','Mod','Sub','Case','If','Cast']) and parent_node.__class__.__name__ in ['Select', 'From', 'Where', 'Group', 'Order','Alias']:
                outer_function_node = current_node
                break
            if parent_node.__class__.__name__ in ['NEQ','EQ','GT','LT']:
                outer_function_node = current_node
                break

            # 继续向上遍历
            current_node = parent_node
            
            parent_node = getattr(parent_node, 'parent', None)
            
        
        return outer_function_node, (outer_function_node is not None)

    def _add_aggregate_node(self, node):
        """添加聚合函数节点到列表"""
        # 生成唯一标识符
        node_id = id(node)

        # 获取函数名称
        if node.__class__.__name__ in [ 'Anonymous']:
            func_name = node.name
        elif node.__class__.__name__ == 'Window':
            func_name = node.this.__class__.__name__
        else:
            func_name = node.__class__.__name__
        
        # 构建节点信息
        node_info = {
            'node_id': node_id,
            'node': node,
            'func_name': func_name,
            'clause_type': 'OTHER',  # 默认值，将在_find_aggregate_nodes中更新
            'expr_str': ''  # 默认值，将在_find_aggregate_nodes中更新
        }
        
        self.aggregate_nodes.append(node_info)
        return node_info

    def mutate(self):
        """对聚合函数执行数学性质等价变换，并比较结果是否相同"""
        if self.mutated:
            return self.ast

        # 获取原始SQL字符串
        original_sql = self.ast.sql(dialect=self._get_sqlglot_dialect_name())
        print(f"原始SQL: {original_sql}")

        # 查找所有聚合函数节点
        self.find_aggregate_nodes(self.ast)
        print(f"找到的聚合函数节点数量: {len(self.aggregate_nodes)}")
        for node_info in self.aggregate_nodes:
            print(node_info)
        # 如果没有函数节点，直接返回
        if not self.aggregate_nodes:
            print("未找到聚合函数节点，无需变异")
            return self.ast

        # 创建执行器实例
        generator = SeedQueryGenerator(db_config=self.db_config)
        initial_result = generator.execute_query(original_sql)
        initial_ast = copy.deepcopy(self.ast)
        print(len(self.aggregate_nodes))
        # 遍历所有聚合函数节点，逐个应用变异
        for i, node_info in enumerate(self.aggregate_nodes):
            print(f"当前处理的原始索引: {i}")
            # 每次从原始AST开始，确保每次只变异一个节点
            self.ast = copy.deepcopy(initial_ast)
            temp_ast = self.ast
            self.find_aggregate_nodes(temp_ast)
            # 找到对应的节点
            index = 0 
            for index, node_in in enumerate(self.aggregate_nodes):
                if i == index:
                    print(f"当前处理的索引: {index}")
                    node_info = node_in
                    break
            
            node = node_info['node']
            func_name = node_info['func_name']
            print(f"当前处理的聚合函数: {func_name}")
            if func_name == 'Anonymous':
                func_name = node.this
                print(f"Anonymous 函数名: {func_name}")
            #=========================================================#   
            # 使用变异器工厂获取合适的变异器
            if self.mutator_factory.supports_mutation(func_name):
                # 重置变异标志
                self.mutations_applied = False
                
                # 获取合适的变异器
                mutator = self.mutator_factory.get_mutator(node, self.ast)
                
                if mutator:
                    # 应用变异
                    print(f"应用变异: {func_name}")
                    # 保存原始节点信息到mutator
                    mutator.node_info = node_info
                    mutator.parent = self
                    # 执行变异并获取变异后的节点
                    mutated_node = mutator.mutate()
                    print(mutated_node)
                    original_outer_node_copy = None
                    if mutated_node and mutated_node != node:
                        # 保存原始节点的外部函数信息（在替换之前）
                        original_outer_node, original_is_nested = self._get_outer_function_node(node)
                        original_outer_node_copy = original_outer_node.copy() if original_outer_node is not None else None
                        dialect_name = self._get_sqlglot_dialect_name()
                        original_str = original_outer_node.sql(dialect=dialect_name)
                        # 使用replace方法替换节点
                        parent_node = node_info['parent_node']
                        node.replace(mutated_node)
                        self.mutations_applied = True
                    print(node_info['clause_type'])
                    # 如果应用了变异，且节点在SELECT子句中，确保GROUP BY子句包含变异后的表达式
                    if self.mutations_applied and node_info['clause_type'] == 'SELECT':
                        # 检查节点是否在嵌套函数中
                        print(f"当前节点: {mutated_node}")
                        print(mutated_node.parent)
                        outer_node, is_nested = self._get_outer_function_node(mutated_node)
                        print(f"当前节点的父节点: {outer_node}")
                        print(f"当前节点的外部函数: {outer_node}")
                        print(f"当前节点的类型: {outer_node.__class__.__name__}")
                        # 需要添加到GROUP BY的表达式
                        expr_to_add = None
                        c_outer_node=outer_node
                        if outer_node.__class__.__name__ in ['Neg','Div']:
                            c_outer_node=outer_node.this
                        if is_nested:
                            if c_outer_node.__class__.__name__ in ['Add','Mod','Sub','Case','If',]:
                                print(f"✓ 变异节点为算数表达式中的子节点，需要将算数表达式加入GROUP BY")
                                expr_to_add=c_outer_node
                            # 情况1：节点是嵌套函数的内部函数
                            elif self._is_scalar_function(c_outer_node):
                                # 外部是非聚合、非窗口函数，将整个外部节点加入GROUP BY
                                expr_to_add = outer_node
                                print(f"✓ 变异节点是嵌套函数的内部函数，外部是非聚合非窗口函数，将整个外部节点 {str(outer_node)} 加入GROUP BY")
                            elif self._is_aggregate_function(c_outer_node):
                                # 外部是聚合函数，不需要添加GROUP BY
                                print(f"✓ 变异节点是嵌套函数的内部函数，外部是聚合函数，不需要添加GROUP BY")
                                expr_to_add = None
                            elif self._is_window_function(c_outer_node):
                                # 外部是窗口函数，将变异节点加入同级GROUP BY
                                expr_to_add = mutated_node
                                print(f"✓ 变异节点是嵌套函数的内部函数，外部是窗口函数，将变异节点 {str(mutated_node)} 加入同级GROUP BY")
                        else:
                            # 情况2：节点不是嵌套函数
                            # 检查是否是非聚合、非窗口函数
                            if self._is_scalar_function(mutated_node):
                                # 非聚合、非窗口函数，将节点加入GROUP BY
                                expr_to_add = mutated_node
                                print(f"✓ 变异节点是非聚合非窗口函数，将节点 {str(mutated_node)} 加入GROUP BY")
                            # 聚合函数和窗口函数不需要添加到GROUP BY
                            elif self._is_aggregate_function(mutated_node):
                                print(f"✓ 变异节点是聚合函数，不需要添加GROUP BY")
                            elif self._is_window_function(mutated_node):
                                print(f"✓ 变异节点是窗口函数，不需要添加GROUP BY")
                        print(expr_to_add)
                        # 如果确定了要添加的表达式
                        if expr_to_add:
                            # 找到对应的SELECT节点
                            # Use the mutated node to locate the current SELECT (parent_node may be stale after replace)
                            select_node = mutated_node or node
                            while hasattr(select_node, 'parent') and select_node.parent and select_node.__class__.__name__ != 'Select':
                                select_node = select_node.parent
                            
                            # 如果找到了SELECT节点
                            if select_node.__class__.__name__ == 'Select':
                                # 使用sqlglot的API处理GROUP BY子句
                                if hasattr(select_node, 'args') and 'group' in select_node.args:
                                    # 获取当前GROUP BY表达式
                                    current_group = select_node.args['group']
                                    
                                    if current_group:
                                        # 检查表达式是否已经在GROUP BY中
                                        expr_str = expr_to_add.sql(dialect=dialect_name)
                                        if expr_str not in [expr.sql(dialect=dialect_name) for expr in current_group.expressions]:
                                            # 添加到现有的GROUP BY表达式列表
                                            current_group.expressions.append(expr_to_add)
                                            print(f"✓ 已将表达式 {expr_str} 添加到GROUP BY子句")
                    elif self.mutations_applied and node_info['clause_type'] == 'GROUP BY':
                        # 将原始节点加入GROUP BY子句中，避免only_full_group_by的错误
                        print(f"✓ 已应用变异且节点在GROUP BY子句中，处理原始节点 {str(node)}")
                        print(original_str)
                        
                        # 确定要添加的节点：如果是嵌套函数的内层函数，需要特殊处理CASE表达式
                        try:
                                # 使用替换前保存的原始节点的外部函数信息
                                outer_node = original_outer_node_copy if original_outer_node_copy is not None else sqlglot.parse_one(original_str, read=dialect_name)
                                is_nested = original_is_nested
                                
                                # 确定要添加的节点：如果是嵌套函数的内层函数，添加整个嵌套函数；否则添加原始节点
                                expr_to_add = outer_node if outer_node is not None else node
                                expr_str = expr_to_add.sql(dialect=dialect_name)
                                print(f"✓ {'嵌套函数的内层函数，将整个嵌套函数' if is_nested else '非嵌套函数，将原始节点'} {expr_str} 加入GROUP BY")
                        except Exception as e:
                            print(f"✗ 解析表达式时出错: {e}")
                            # 出错时，使用原始节点作为备选
                            expr_to_add = node
                            expr_str = expr_to_add.sql(dialect=dialect_name)
                            print(f"✓ 使用备选方案，将原始节点 {expr_str} 加入GROUP BY")
                        
                        # 找到对应的SELECT节点
                        print(parent_node)
                        select_node = parent_node
                        while hasattr(select_node, 'parent') and select_node.parent and select_node.__class__.__name__ != 'Select':
                            select_node = select_node.parent
                        
                        # 如果找到了SELECT节点
                        if select_node.__class__.__name__ == 'Select':
                            # 使用sqlglot的API处理GROUP BY子句
                            if hasattr(select_node, 'args') and 'group' in select_node.args:
                                # 获取当前GROUP BY表达式
                                current_group = select_node.args['group']
                                
                                if current_group:
                                    # 检查表达式是否已经在GROUP BY中
                                    if expr_str not in [expr.sql(dialect=dialect_name) for expr in current_group.expressions]:
                                        # 添加到现有的GROUP BY表达式列表
                                        current_group.expressions.append(expr_to_add)
                                        print(f"✓ 已将表达式 {expr_str} 添加到GROUP BY子句")
                            else:
                                # 创建新的GROUP BY子句，包含原始节点
                                from sqlglot import exp
                                group_expr = exp.Group(expressions=[node])
                                select_node.args['group'] = group_expr
                                print(f"✓ 已创建GROUP BY子句并添加原始节点 {str(node)}")
                    else:
                        print(f"未生成有效的变异节点: {func_name}")
                else:
                    print(f"未找到合适的变异器: {func_name}")
            else:
                print(f"不支持变异的函数: {func_name}")
       
        #=========================================================#   


            # 检查是否应用了任何变异
            print(self.ast)
            # 获取变异后的SQL，使用sql()方法确保正确的语法格式
            mutated_sql = self.ast.sql(dialect=self._get_sqlglot_dialect_name())
            # 比较原始SQL和变异SQL的执行结果
            results_match = self.compare_results(original_sql, mutated_sql)
            
            # 如果结果相同，接受变异
            if results_match:
                self.mutated = True
                print("结果相同，接受变异")
            else:
                print("结果不匹配，拒绝变异")
                #print("程序将终止运行")
                #sys.exit(1)
                
                # 保留原始AST
                self.ast = initial_ast
            
        return self.ast
    
    def _is_count_star(self, count_node):
        """检查COUNT函数是否是COUNT(*))"""
        if hasattr(count_node, 'this') and count_node.this:
            return count_node.this.__class__.__name__ == 'Star'
        return False

    def _is_date_column(self, node):
        """检查节点是否为纯日期类型列 - 仅使用表结构信息"""

        
        if node is None:

            return False
        
        # 获取列名
        column_name = str(node)

        
        # 从表结构信息中获取列类型
        column_info = self._get_column_info(column_name)
        
        # 检查列类型是否为日期类型
        # 实际应用中应该根据数据库的类型系统进行判断
        # 这里我们假设column_info['type']包含了列的实际类型
        result = False
        if column_info['type']:
            result = 'date' in column_info['type'].lower() and 'time' not in column_info['type'].lower()
        return result
            
    def _is_datetime_column(self, node):
        """检查节点是否为日期时间类型列 - 仅使用表结构信息"""

        
        if node is None:

            return False
        
        # 获取列名
        column_name = str(node)

        
        # 从表结构信息中获取列类型
        column_info = self._get_column_info(column_name)
        
        # 检查列类型是否为日期时间类型
        # 实际应用中应该根据数据库的类型系统进行判断
        # 这里我们假设column_info['type']包含了列的实际类型
        result = False
        if column_info['type']:
            result = ('datetime' in column_info['type'].lower() or 
                     'timestamp' in column_info['type'].lower() or 
                     ('date' in column_info['type'].lower() and 'time' in column_info['type'].lower()))
        return result
        
    
    def _is_string_column(self, node):
        """检查节点是否为字符串类型列 - 优先使用表结构信息和Column对象的category属性"""

        
        if node is None:

            return False
        
        # 获取列名
        column_name = str(node)

        
        # 从表结构信息中获取列类型
        column_info = self._get_column_info(column_name)
        
        # 优先检查是否为数值类型（通过category）
        result = False
        if column_info.get('is_numeric') is not None:
            # 如果已经确认是数值类型，则不是字符串类型
            result = not column_info['is_numeric']

        
        # 如果有列类型信息，使用关键词判断
        elif column_info['type']:
            result = ('char' in column_info['type'].lower() or 
                     'text' in column_info['type'].lower() or 
                     'string' in column_info['type'].lower())

        return result

    def _get_function_return_type(self, func_name, param_type):
        """根据函数名和参数类型推断函数的返回类型
        
        Args:
            func_name: 函数名称
            param_type: 参数类型
            
        Returns:
            bool: 函数返回值是否为数值类型
        """
        # 处理函数名的大小写
        func_name_lower = func_name.lower()
        
        # 检查函数是否在函数映射中
        if func_name_lower in self.function_map:
            func = self.function_map[func_name_lower]
            return_type = func.return_type.lower()
            
            # 判断返回类型是否为数值类型
            return any(keyword in return_type for keyword in ['numeric', 'double', 'int', 'float', 'decimal', 'real'])
        
        # 特殊处理COUNT函数（在create_sample_functions中被标记为INT类型，但通常不参与数值运算）
        if func_name_lower in ['count', 'count_distinct']:
            return False
        
        # 默认假设返回数值类型
        return True
    
    def _analyze_nested_function(self, expression):
        """递归分析嵌套函数，推断最外层函数参数的类型
        
        Args:
            expression: 嵌套函数表达式，如"MAX(SUM(amount))"
            
        Returns:
            dict: 包含函数参数类型分析结果的字典
        """
        import sqlglot
        
        # 如果表达式已经是简单列名，直接返回列信息
        if not any(char in expression for char in ['(', ')']):
            column_info = self._get_column_info(expression)
            return {
                'is_numeric': column_info['is_numeric'],
                'final_param': expression,
                'param_type': column_info['type'],
                'analysis_path': [f"直接列: {expression} (类型: {'数值' if column_info['is_numeric'] else '非数值'})"]
            }
        
        try:
            # 解析表达式为AST
            ast = sqlglot.parse_one(expression)
            
            analysis_path = []
            
            # 递归分析函数调用树
            def analyze_function_tree(node):
                # 获取当前函数名
                func_name = node.__class__.__name__
                if func_name == 'Anonymous' and hasattr(node, 'this'):
                    func_name = node.this
                
                analysis_path.append(f"进入函数: {func_name}")
                
                # 检查是否有参数
                if hasattr(node, 'this'):
                    param_node = node.this
                    param_str = str(param_node)
                    
                    # 检查参数是否是另一个函数调用
                    if hasattr(param_node, '__class__'):
                        param_class = param_node.__class__.__name__
                        
                        # 特殊处理DISTINCT情况
                        if param_class == 'Distinct':
                            if hasattr(param_node, 'expressions') and param_node.expressions:
                                inner_param = param_node.expressions[0]
                                analysis_path.append(f"处理DISTINCT: {str(inner_param)}")
                                # 递归分析DISTINCT内的参数
                                inner_result = analyze_function_tree(inner_param)
                                # DISTINCT不改变参数类型
                                return {
                                    'is_numeric': inner_result['is_numeric'],
                                    'final_param': inner_result['final_param'],
                                    'param_type': inner_result['param_type'],
                                    'analysis_path': analysis_path
                                }
                        
                        # 如果参数是另一个函数调用，递归分析
                        if (param_class not in ['Column', 'Identifier', 'Literal', 'Table', 'From', 'Select', 
                                             'Selectable', 'Expr', 'Star', 'Alias']):
                            # 递归分析内部函数
                            inner_result = analyze_function_tree(param_node)
                            
                            # 推断当前函数的返回类型
                            current_returns_numeric = self._get_function_return_type(func_name, inner_result['param_type'])
                            analysis_path.append(f"函数 {func_name} 返回类型: {'数值' if current_returns_numeric else '非数值'}")
                            
                            return {
                                'is_numeric': current_returns_numeric,
                                'final_param': inner_result['final_param'],
                                'param_type': inner_result['param_type'],
                                'analysis_path': analysis_path
                            }
                    
                    # 参数是简单表达式（列或常量）
                    column_info = self._get_column_info(param_str)
                    analysis_path.append(f"参数: {param_str} (类型: {'数值' if column_info['is_numeric'] else '非数值'})")
                    
                    # 推断当前函数的返回类型
                    current_returns_numeric = self._get_function_return_type(func_name, column_info['type'])
                    analysis_path.append(f"函数 {func_name} 返回类型: {'数值' if current_returns_numeric else '非数值'}")
                    
                    return {
                        'is_numeric': current_returns_numeric,
                        'final_param': param_str,
                        'param_type': column_info['type'],
                        'analysis_path': analysis_path
                    }
                
                # 默认情况
                return {
                    'is_numeric': False,
                    'final_param': str(node),
                    'param_type': None,
                    'analysis_path': analysis_path
                }
            
            # 从根节点开始分析
            result = analyze_function_tree(ast)
            
            # 添加分析完成信息
            result['analysis_path'].append(f"最终结论: {expression} 的参数类型为{'数值' if result['is_numeric'] else '非数值'}")
            
            return result
            
        except Exception as e:
            print(f"解析表达式 {expression} 时出错: {str(e)}")
            # 出错时返回默认值
            return {
                'is_numeric': False,
                'final_param': expression,
                'param_type': None,
                'analysis_path': [f"解析错误: {str(e)}"]
            }
    
    def _get_column_info(self, column_name):

        print(f"原始列名: {column_name}")
        
        # 首先检查是否是嵌套函数表达式
        if any(char in column_name for char in ['(', ')']):
            # 使用嵌套函数分析器
            nested_analysis = self._analyze_nested_function(column_name)
            print(f"嵌套函数分析结果:")
            for step in nested_analysis['analysis_path']:
                print(f"  {step}")
            
            # 如果分析结果显示是数值类型，直接返回
            if nested_analysis['is_numeric']:
                return {
                    'name': column_name,
                    'type': nested_analysis['param_type'],
                    'has_nulls': False,  # 默认假设
                    'is_numeric': True
                }
            
            # 如果不是数值类型，或者分析失败，继续使用传统方法
            inner_column = nested_analysis['final_param']
            print(f"提取的最内层列名: {inner_column}")
        else:
            inner_column = column_name
        
        # 将列名转换为小写以进行不区分大小写的比较
        column_name_lower = inner_column.lower()
        
        # 检查并移除distinct关键字
        if 'distinct' in column_name_lower:

            # 提取distinct后面的实际列名
            column_name_lower = column_name_lower.replace('distinct', '').strip()

        
        # 初始化column_to_check变量
        column_to_check = column_name_lower
        print(self.alias_map)
        # 检查别名映射
        if self.alias_map:
            # 处理table.column形式的列名，先查找表的别名
            if '.' in column_name_lower:
                table_part, column_part = column_name_lower.split('.', 1)

                
                # 先查找表的别名
                if table_part in self.alias_map:
                    actual_table = self.alias_map[table_part]
                    print(f"表别名映射: {table_part} -> {actual_table}")
                    
                    # 构建带实际表名的列名
                    aliased_column = f"{actual_table}.{column_part}"
                    column_to_check = aliased_column

                
                # 然后检查完整的table.column形式的列名是否有别名
                if column_to_check in self.alias_map:
                    actual_column = self.alias_map[column_to_check]
                    column_to_check = actual_column
            
            # 如果之前的处理没有找到，或者是简单列名，直接检查整个列名
            elif column_name_lower in self.alias_map:
                actual_column = self.alias_map[column_name_lower]

                column_to_check = actual_column
        
        # 获取列的信息
        column_info = {
            'name': column_name,
            'type': None,
            'has_nulls': False,
            'is_numeric': False
        }
        print(f"检查列: {column_to_check}")
        print(f"全局表结构: {self.tables}")
        # 从全局表结构信息中获取列类型
        if self.tables:
            found = False
            # 先检查column_to_check是否包含表名
            target_table = None
            target_column = column_to_check
            
            if '.' in column_to_check:
                # 拆分为表名.列名
                table_part, column_part = column_to_check.split('.', 1)
                
                target_table = table_part
                target_column = column_part
                
            else:
                target_column = column_to_check
            
            # 优先在指定的表中查找列
            if target_table:

                for table in self.tables:
                    if table.name.lower() == target_table:

                        for column in table.columns:
                            if column.name.lower() == target_column:
    
                                column_info['type'] = column.data_type
                                column_info['has_nulls'] = column.is_nullable
                                column_info['is_numeric'] = column.category == 'numeric'
                                found = True
                                break
                        break
                
                if found:
                    pass
            
            # 如果在指定表中未找到，或者没有指定表，则进行全局查找
            if not found:

                for table in self.tables:

                    table_has_column = False
                    for column in table.columns:
                        if column.name.lower() == column_to_check or (target_column and column.name.lower() == target_column):
    
                            column_info['type'] = column.data_type
                            column_info['has_nulls'] = column.is_nullable
                            column_info['is_numeric'] = column.category == 'numeric'
                            found = True
                            table_has_column = True
                            break
                    if not table_has_column:
                        pass
                    if found:
    
                        break
            
            if not found:
                pass

        
            
        
        
        return column_info



    def get_mutated_ast(self):
        """获取变异后的AST"""
        if not self.mutated:
            return self.ast
        return self.ast
    
    
    def compare_results(self, original_sql, mutated_sql):
        """比较原始SQL和变异SQL的执行结果是否相同
        
        参数:
        - original_sql: 原始SQL字符串
        - mutated_sql: 变异后的SQL字符串
        
        返回:
        - bool: 如果原始结果的每一条数据都可以在变异结果集中找到，并且列名严格相等，返回True，否则返回False
        """
        # 创建执行器实例
        executor = SeedQueryGenerator(db_config=self.db_config)
        
        try:
            # 执行原始SQL
            print(f"执行原始SQL: {original_sql}")
            original_result_data = executor.execute_query(original_sql)
            
            # 执行变异SQL
            print(f"执行变异SQL: {mutated_sql}")
            mutated_result_data = executor.execute_query(mutated_sql)
            
            # 解包结果数据
            if isinstance(original_result_data, tuple) and len(original_result_data) == 2:
                original_result = original_result_data[0]  # 结果集
                original_column_names = original_result_data[1]  # 列名列表
            else:
                original_result = original_result_data
                original_column_names = []
            
            if isinstance(mutated_result_data, tuple) and len(mutated_result_data) == 2:
                mutated_result = mutated_result_data[0]  # 结果集
                mutated_column_names = mutated_result_data[1]  # 列名列表
            else:
                mutated_result = mutated_result_data
                mutated_column_names = []
            
            # 检查原始结果集是否为空
            original_result_size = len(original_result) if original_result else 0
            if original_result_size == 0:
                # 当原始结果为空时，检查变异结果是否也为空
                mutated_result_size = len(mutated_result) if mutated_result else 0
                if mutated_result_size == 0:
                    print("原始结果集为空，变异结果集也为空，结果匹配")
                    return True
                else:
                    print("原始结果集为空，但变异结果集不为空，记录无效变异")
                    return True
                    self._log_invalid_mutation(original_sql, mutated_sql, original_result, 
                                            mutated_result, original_column_names, 
                                            mutated_column_names, "原始结果集为空但变异结果集不为空")
                    return False
                    
            
            # 检查变异结果集是否为空
            mutated_result_size = len(mutated_result) if mutated_result else 0
            if mutated_result_size == 0:
                print("变异结果集为空，无法找到匹配数据")
                return True
                self._log_invalid_mutation(original_sql, mutated_sql, original_result, 
                                        mutated_result, original_column_names, 
                                        mutated_column_names, "变异结果集为空")
                return False
                
            
            # 比较列名
            
            if original_column_names != mutated_column_names:
                print(f"结果不匹配 - 列名不一致: {original_column_names} vs {mutated_column_names}")
                self._log_invalid_mutation(original_sql, mutated_sql, original_result, 
                                        mutated_result, original_column_names, 
                                        mutated_column_names, "列名不一致")
                return False
            
            # 比较列数量
            if original_result and mutated_result:
                orig_row_len = len(original_result[0]) if original_result[0] else 0
                mut_row_len = len(mutated_result[0]) if mutated_result[0] else 0
                
                if orig_row_len != mut_row_len:
                    print(f"结果不匹配 - 每行的列数不一致: {orig_row_len} vs {mut_row_len}")
                    self._log_invalid_mutation(original_sql, mutated_sql, original_result, 
                                            mutated_result, original_column_names, 
                                            mutated_column_names, "每行的列数不一致")
                    return False
            
            # 将结果集转换为列表以便比较
            original_result_list = []
            if original_result:
                for row in original_result:
                    original_result_list.append(row)
            
            mutated_result_list = []
            if mutated_result:
                for row in mutated_result:
                    mutated_result_list.append(row)
            
            # 找出ROW_NUMBER()、RANK()、DENSE_RANK()函数生成的列索引
            row_number_columns = []
            if original_column_names:
                for i, col_name in enumerate(original_column_names):
                    # 检查列名或SQL中是否包含窗口函数关键词
                    if ('row_number' in col_name.lower() or 'row_number' in str(original_sql).lower() or 
                        'rank' in col_name.lower() or 'rank()' in str(original_sql).lower() or 
                        'dense_rank' in col_name.lower() or 'dense_rank()' in str(original_sql).lower() or
                        'lead' in col_name.lower() or 'lead()' in str(original_sql).lower() or
                        'lag' in col_name.lower() or 'lag()' in str(original_sql).lower()):
                        row_number_columns.append(i)
            
            # 检查原始结果的每一条数据是否都可以在变异结果集中找到
            all_rows_found = True
            for orig_row in original_result_list:
                found_match = False
                
                # 在变异结果集中查找匹配的行
                for mut_row in mutated_result_list:
                    if self._rows_match(orig_row, mut_row, row_number_columns):
                        found_match = True
                        break
                
                if not found_match:
                    all_rows_found = False
                    print(f"结果不匹配 - 原始结果中的行在变异结果集中未找到: {orig_row}")
                    break
            
            if not all_rows_found:
                self._log_invalid_mutation(original_sql, mutated_sql, original_result, 
                                        mutated_result, original_column_names, 
                                        mutated_column_names, "原始结果中的某些行在变异结果集中未找到")
                return False
            
            # 所有比较都通过
            print("结果比较通过 - 原始SQL结果的每一条数据都可以在变异SQL结果中找到")
            return True
            
        except Exception as e:
            print(f"比较结果时发生错误: {str(e)}")
            # 创建invalid_mutation文件夹（如果不存在）
            log_dir = 'invalid_mutation'
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            # 获取当前类名作为日志文件名的一部分
            class_name = self.__class__.__name__
            log_filename = f"{log_dir}/{class_name}_invalid_mutations.log"
            
            # 记录错误信息
            with open(log_filename, 'a', encoding='utf-8') as f:
                f.write(f"=== 比较结果时发生错误 ===\n")
                f.write(f"原始SQL: {original_sql}\n")
                f.write(f"变异SQL: {mutated_sql}\n")
                f.write(f"错误信息: {str(e)}\n\n")
            return False
        finally:
            self._close_geometry_compare_connection()

    _NON_JSON_SENTINEL = object()
    _GEOMETRY_HEX_RE = re.compile(r"^[0-9a-fA-F]+$")
    _GEOMETRY_WKT_PREFIXES = (
        "POINT",
        "LINESTRING",
        "POLYGON",
        "MULTIPOINT",
        "MULTILINESTRING",
        "MULTIPOLYGON",
        "GEOMETRYCOLLECTION",
    )
    _WKB_TYPE_IDS = {1, 2, 3, 4, 5, 6, 7}

    def _normalize_geometry_value(self, value):
        if value is None:
            return None
        if isinstance(value, memoryview):
            value = value.tobytes()
        if isinstance(value, (bytes, bytearray)):
            if not value:
                return None
            return ("wkb", bytes(value))
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if (text.startswith("0x") or text.startswith("0X")) and self._GEOMETRY_HEX_RE.match(text[2:]):
            if len(text[2:]) % 2 == 0:
                return ("wkb_hex", text[2:].lower())
        if (text.startswith("x'") or text.startswith("X'")) and text.endswith("'"):
            hex_body = text[2:-1]
            if self._GEOMETRY_HEX_RE.match(hex_body) and len(hex_body) % 2 == 0:
                return ("wkb_hex", hex_body.lower())

        srid = None
        wkt_text = text
        if text.upper().startswith("SRID=") and ";" in text:
            srid_text, wkt_candidate = text.split(";", 1)
            try:
                srid = int(srid_text[5:])
                wkt_text = wkt_candidate.strip()
            except ValueError:
                srid = None
                wkt_text = text
        wkt_prefix = wkt_text.lstrip().upper()
        for prefix in self._GEOMETRY_WKT_PREFIXES:
            if wkt_prefix.startswith(prefix):
                return ("wkt", wkt_text, srid)
        return None

    def _strip_mysql_srid_prefix(self, raw):
        if len(raw) < 5:
            return None
        if raw[4] not in (0, 1):
            return None
        wkb = raw[4:]
        byte_order = wkb[0]
        if byte_order not in (0, 1):
            return None
        geom_type = int.from_bytes(wkb[1:5], "little" if byte_order == 1 else "big")
        if geom_type not in self._WKB_TYPE_IDS:
            return None
        srid = int.from_bytes(raw[:4], "little")
        return (wkb, srid)

    def _strip_mysql_srid_prefix_hex(self, hex_text):
        try:
            raw = bytes.fromhex(hex_text)
        except ValueError:
            return None
        stripped = self._strip_mysql_srid_prefix(raw)
        if not stripped:
            return None
        wkb, srid = stripped
        return (wkb.hex(), srid)

    def _geometry_candidates(self, value):
        norm = self._normalize_geometry_value(value)
        if not norm:
            return []
        candidates = [norm]
        if norm[0] == "wkb":
            stripped = self._strip_mysql_srid_prefix(norm[1])
            if stripped:
                candidates.append(("wkb_srid", stripped[0], stripped[1]))
        elif norm[0] == "wkb_hex":
            stripped = self._strip_mysql_srid_prefix_hex(norm[1])
            if stripped:
                candidates.append(("wkb_hex_srid", stripped[0], stripped[1]))
        return candidates

    def _geometry_sql_fragment(self, norm_value):
        kind = norm_value[0]
        if kind == "wkb":
            return "ST_GeomFromWKB(%s)", [norm_value[1]]
        if kind == "wkb_srid":
            return "ST_GeomFromWKB(%s, %s)", [norm_value[1], norm_value[2]]
        if kind == "wkb_hex":
            return "ST_GeomFromWKB(UNHEX(%s))", [norm_value[1]]
        if kind == "wkb_hex_srid":
            return "ST_GeomFromWKB(UNHEX(%s), %s)", [norm_value[1], norm_value[2]]
        if kind == "wkt":
            wkt_text, srid = norm_value[1], norm_value[2]
            if srid is not None:
                return "ST_GeomFromText(%s, %s)", [wkt_text, srid]
            return "ST_GeomFromText(%s)", [wkt_text]
        return None, []

    def _get_geometry_compare_connection(self):
        if self._geometry_compare_connection:
            try:
                self._geometry_compare_connection.ping(reconnect=True)
                return self._geometry_compare_connection
            except Exception:
                self._close_geometry_compare_connection()
        generator = SeedQueryGenerator(db_config=self.db_config)
        self._geometry_compare_connection = generator.connect_db()
        return self._geometry_compare_connection

    def _close_geometry_compare_connection(self):
        if self._geometry_compare_connection:
            try:
                self._geometry_compare_connection.close()
            except Exception:
                pass
            self._geometry_compare_connection = None

    def _geometry_pair_equal(self, norm_orig, norm_mut, conn):
        if norm_orig == norm_mut:
            return True
        cache_key = (norm_orig, norm_mut)
        if cache_key in self._geometry_compare_cache:
            return self._geometry_compare_cache[cache_key]
        reverse_key = (norm_mut, norm_orig)
        if reverse_key in self._geometry_compare_cache:
            return self._geometry_compare_cache[reverse_key]

        sql_left, params_left = self._geometry_sql_fragment(norm_orig)
        sql_right, params_right = self._geometry_sql_fragment(norm_mut)
        if not sql_left or not sql_right:
            return None

        result = None
        try:
            sql = (
                "SELECT ST_Equals(g1, g2) AS geom_equal, "
                "ST_SRID(g1) AS srid_1, ST_SRID(g2) AS srid_2 "
                f"FROM (SELECT {sql_left} AS g1, {sql_right} AS g2) AS geom_cmp"
            )
            params = params_left + params_right
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()
            if row:
                geom_equal = bool(row[0])
                if geom_equal and row[1] is not None and row[2] is not None:
                    geom_equal = row[1] == row[2]
                result = geom_equal
        except Exception:
            result = None

        if result is not None:
            self._geometry_compare_cache[cache_key] = result
            self._geometry_compare_cache[reverse_key] = result
        return result

    def _geometry_values_equal(self, orig_val, mut_val):
        candidates_orig = self._geometry_candidates(orig_val)
        candidates_mut = self._geometry_candidates(mut_val)
        if not candidates_orig or not candidates_mut:
            return None
        for norm_orig in candidates_orig:
            for norm_mut in candidates_mut:
                if norm_orig == norm_mut:
                    return True

        conn = self._get_geometry_compare_connection()
        if not conn:
            return None

        had_result = False
        for norm_orig in candidates_orig:
            for norm_mut in candidates_mut:
                result = self._geometry_pair_equal(norm_orig, norm_mut, conn)
                if result is True:
                    return True
                if result is False:
                    had_result = True
        if had_result:
            return False
        return None

    def _parse_json_value(self, value):
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, (bytes, bytearray)):
            try:
                value = value.decode('utf-8')
            except Exception:
                value = value.decode('utf-8', errors='ignore')
        if not isinstance(value, str):
            return self._NON_JSON_SENTINEL
        text = value.strip()
        if not text:
            return self._NON_JSON_SENTINEL
        try:
            return json.loads(text)
        except (TypeError, ValueError):
            return self._NON_JSON_SENTINEL

    def _json_values_equal(self, orig_val, mut_val):
        orig_json = self._parse_json_value(orig_val)
        if orig_json is self._NON_JSON_SENTINEL:
            return False
        mut_json = self._parse_json_value(mut_val)
        if mut_json is self._NON_JSON_SENTINEL:
            return False
        return orig_json == mut_json

    def _values_match_non_numeric(self, orig_val, mut_val):
        if (orig_val is None and mut_val == '') or (orig_val == '' and mut_val is None):
            return True
        if self._json_values_equal(orig_val, mut_val):
            return True
        geometry_equal = self._geometry_values_equal(orig_val, mut_val)
        if geometry_equal is True:
            return True
        if geometry_equal is False:
            return False
        return orig_val == mut_val

    def _floats_close(self, orig_float, mut_float):
        if orig_float == mut_float:
            return True
        if math.isnan(orig_float) and math.isnan(mut_float):
            return True
        return math.isclose(orig_float, mut_float, rel_tol=1e-12, abs_tol=1e-4)

    def _rows_match(self, orig_row, mut_row, row_number_columns=None):
        """检查两行数据是否匹配
        
        参数:
        - orig_row: 原始SQL的一行结果
        - mut_row: 变异SQL的一行结果
        - row_number_columns: ROW_NUMBER()函数生成的列索引列表，这些列将被排除在比较之外
        
        返回:
        - bool: 如果两行数据匹配，返回True，否则返回False
        """
        # 处理SUM→AVG*COUNT变异的特殊情况，允许一定的波动幅度
        if self.is_sum_to_avg_count_mutation:
            # 确保两行长度相同
            if len(orig_row) != len(mut_row):
                return False
                
            for j, (orig_val, mut_val) in enumerate(zip(orig_row, mut_row)):
                # 如果是ROW_NUMBER()列，跳过比较
                if row_number_columns and j in row_number_columns:
                    continue
                
                # 尝试进行浮点数比较
                try:
                    orig_float = float(orig_val)
                    mut_float = float(mut_val)
                    
                    # 计算波动幅度
                    if orig_float == 0:
                        # 避免除以零，如果原值为0，则检查变异值是否也接近0
                        if abs(mut_float) > 0.05:
                            return False
                    else:
                        fluctuation = abs((mut_float - orig_float) / orig_float)
                        if fluctuation > 0.05:
                            return False
                except (ValueError, TypeError):
                    # 无法转换为浮点数的情况，将None和空字符串视为相同
                    # 检查None和空字符串的特殊情况
                    if not self._values_match_non_numeric(orig_val, mut_val):
                        return False
                        
            # 所有列都匹配
            return True
        elif self.is_parameter_equivalence_mutation:
            # 处理参数等价变换的特殊情况，这些变换应该在数学上是等价的
            # 确保两行长度相同
            if len(orig_row) != len(mut_row):
                return False
                
            for j, (orig_val, mut_val) in enumerate(zip(orig_row, mut_row)):
                # 如果是ROW_NUMBER()列，跳过比较
                if row_number_columns and j in row_number_columns:
                    continue
                
                # 尝试进行浮点数比较，参数等价变换（如c→c+0）应该结果完全相同
                try:
                    orig_float = float(orig_val)
                    mut_float = float(mut_val)
                    
                    # 允许浮点误差（绝对+相对）
                    if not self._floats_close(orig_float, mut_float):
                        return False
                except (ValueError, TypeError):
                    # 无法转换为浮点数的情况，将None和空字符串视为相同
                    # 检查None和空字符串的特殊情况
                    if not self._values_match_non_numeric(orig_val, mut_val):
                        return False
                        
            # 所有列都匹配
            return True
        else:
            # 普通变异
            if len(orig_row) != len(mut_row):
                return False
            
            for j, (orig_val, mut_val) in enumerate(zip(orig_row, mut_row)):
                # 如果是ROW_NUMBER()列，跳过比较
                if row_number_columns and j in row_number_columns:
                    continue
                
                try:
                    orig_float = float(orig_val)
                    mut_float = float(mut_val)
                    
                    # 允许浮点误差（绝对+相对）
                    if not self._floats_close(orig_float, mut_float):
                        return False
                except (ValueError, TypeError):
                    # 无法转换为浮点数的情况，将None和空字符串视为相同
                    # 检查None和空字符串的特殊情况
                    if not self._values_match_non_numeric(orig_val, mut_val):
                        return False
            
            return True
            

    def _log_invalid_mutation(self, original_sql, mutated_sql, original_result, 
                            mutated_result, original_column_names, 
                            mutated_column_names, reason):
        """记录不符合预期的变异到日志文件"""
        # 获取当前数据库方言
        from data_structures.db_dialect import get_current_dialect
        dialect = get_current_dialect()
        db_type = dialect.name.upper()
        
        # 创建invalid_mutation/{db_type}文件夹（如果不存在）
        log_dir = f'invalid_mutation/{db_type}'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # 获取当前类名作为日志文件名的一部分，并添加变异类别
        class_name = self.__class__.__name__
        
        # 确定变异类别
        mutation_category = "数学性质等价变换"
        
        # 生成日志文件名
        log_filename = f"{log_dir}/{class_name}_{db_type}_invalid_mutations.log"
        
        # 获取原始查询的索引使用情况
        def _get_query_index_info(self, sql_query):
            """执行EXPLAIN SQL获取查询的索引使用情况
            
            参数:
                sql_query: 要分析的SQL查询
            
            返回:
                str: 索引使用情况的文本描述
            """
            try:
                # 直接导入并使用get_seedQuery中的SeedQueryGenerator类
                from get_seedQuery import SeedQueryGenerator
                
                # 创建SeedQueryGenerator实例
                seed_generator = SeedQueryGenerator(db_config=self.db_config)
                
                # 使用SeedQueryGenerator的connect_db方法获取数据库连接
                connection = seed_generator.connect_db()
                
                if not connection:
                    return "无法建立数据库连接"
                
                # 执行EXPLAIN查询
                explain_sql = f"EXPLAIN {sql_query}"
                cursor = connection.cursor()
                cursor.execute(explain_sql)
                
                # 获取结果
                explain_results = cursor.fetchall()
                
                # 关闭游标和连接
                cursor.close()
                connection.close()
                
                # 格式化结果为可读文本
                index_info = []
                
                # 尝试获取列名
                try:
                    # 注意：这里可能需要调整，因为cursor.description在fetchall后可能不可用
                    column_names = [desc[0] for desc in cursor.description]
                    for row in explain_results:
                        row_info = []
                        for i, val in enumerate(row):
                            col_name = column_names[i] if i < len(column_names) else f"col_{i}"
                            row_info.append(f"{col_name}: {val}")
                        index_info.append(" | ".join(row_info))
                except Exception:
                    # 如果无法获取列名，直接输出结果
                    for row in explain_results:
                        index_info.append(str(row))
                
                return "\n".join(index_info)
            except Exception as e:
                return f"获取索引信息失败: {str(e)}"
        
        index_info = _get_query_index_info(self, original_sql)
        
        # 写入日志
        with open(log_filename, 'a', encoding='utf-8') as f:
            f.write(f"=== {mutation_category} 结果不匹配 ({db_type}) ===\n")
            f.write(f"原始SQL: {original_sql}\n")
            f.write(f"变异SQL: {mutated_sql}\n")
            f.write(f"原始查询索引使用情况:\n{index_info}\n")
            f.write(f"原始结果集大小: {len(original_result) if original_result else 0}\n")
            f.write(f"变异结果集大小: {len(mutated_result) if mutated_result else 0}\n")
            f.write(f"原始列名: {original_column_names}\n")
            f.write(f"变异列名: {mutated_column_names}\n")
            f.write(f"失败原因: {reason}\n")
            f.write(f"原始结果集: {original_result}\n")
            f.write(f"变异结果集: {mutated_result}\n\n")
    
    




