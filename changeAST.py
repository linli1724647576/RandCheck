import ast
import os
import sqlglot
import copy
import random
import string
from get_seedQuery import SeedQueryGenerator
from generateAST import Change
from mutator.aggregate_math_equivalence_mutator import AggregateMathEquivalenceMutator
from mutator.slot_driven_mutation_pipeline import SlotDrivenMutationPipeline
from mutator.result_comparator import ResultComparator
from data_structures.db_dialect import get_current_dialect


def _get_sqlglot_dialect_name() -> str:
    """Map current dialect to sqlglot dialect name."""
    dialect = get_current_dialect()
    if dialect and dialect.name.upper() == "POSTGRESQL":
        return "postgres"
    return "mysql"



class PreSolve:
    def __init__(self, file_path="./generated_sql/seedQuery.sql", extension=False, db_config=None):
        self.change = Change()
        self.file_path = file_path
        self.extension = extension
        self.db_config = db_config
        # 获取当前设置的数据库方言
        dialect = get_current_dialect()
        self.db_type = dialect.name if dialect else 'UNKNOWN'
        # 不再在初始化时加载所有查询，而是使用迭代器按需加载

    def seed_query_iterator(self):
        """使用生成器逐行读取种子查询文件
        
        返回:
        - 生成器: 逐个返回种子查询（保持与原始get_queries方法一致）
        """
        try:
            # 获取绝对路径
            abs_path = os.path.abspath(self.file_path)

            with open(abs_path, 'r', encoding='utf-8') as f:
                # 逐行读取文件
                for line in f:
                    # 去除行首尾的空白字符
                    sql = line.strip()
                    # 忽略空行
                    if sql:
                        yield sql
        except Exception as e:
            print(f"读取种子查询文件时出错: {e}")

    def presolve(self, batch_size=100, max_queries=None, aggregate_mutation_type=None):
        """处理种子查询（分批处理，减少内存占用）
        
        参数:
        - batch_size: 每批处理的查询数量
        - max_queries: 最大处理的查询数量（None表示处理所有）
        - aggregate_mutation_type: 聚合函数变异类型 (None, 'normal', 'math_equivalence')
        """
        executor = SeedQueryGenerator(db_config=self.db_config)
        
        # 统计处理的查询数量
        processed_count = 0
        batch_count = 0
        slot_pipeline = None
        comparator = ResultComparator(db_config=self.db_config) if self.extension and aggregate_mutation_type in {'slot_m4'} else None
        if self.extension and aggregate_mutation_type in {'slot_m1', 'slot_m4'}:
            slot_pipeline = SlotDrivenMutationPipeline()
        
        # 使用迭代器逐个处理查询
        for query in self.seed_query_iterator():
            # 检查是否达到最大处理数量
            if max_queries is not None and processed_count >= max_queries:
                break
            print(query)
            try:
                # 打印进度
                processed_count += 1
                if processed_count % 50 == 0:
                    print(f"已处理 {processed_count} 个种子查询")
                
                # 解析查询为AST
                ast = self.change.ASTChange(query)
                if ast is None:
                    print("解析失败，跳过该查询")
                    continue
                sql = ast.sql(
                    dialect=_get_sqlglot_dialect_name(),
                    normalize=False,
                    normalize_functions=False,
                )
                print(f"=====原始SQL:{sql}======")
                
                if not self.extension:
                    # 对AST进行预处理
                    print(ast)
                    # 不使用ASTMutator，直接跳过
                    print("跳过AST变异，该功能已移除")
                elif self.extension and aggregate_mutation_type == 'math_equivalence':
                    # 使用数学性质等价变换进行聚合函数变异
                    print("聚合函数数学性质等价变换")
                    # 对AST进行数学等价变异的预处理，删除LIMIT语句
                    ast = self.preprocess_math_equivalence(ast)
                    # 执行变异
                    print(ast)
                    mutator = AggregateMathEquivalenceMutator(
                        ast,
                        db_config=self.db_config,
                    )
                    mutator.mutate()
                    print(f"数学性质等价变换完成")
                elif self.extension and aggregate_mutation_type == 'slot_m1':
                    # M1: parser + slot extractor
                    result = slot_pipeline.analyze_query(query)
                    if not result.success:
                        print(f"M1槽位提取失败: {result.error}")
                        continue
                    print(f"M1槽位提取完成，槽位数量: {len(result.slots)}")
                    for slot in result.slots:
                        print(f"  SLOT {slot.slot_id}: {slot.to_dict()}\n")
                elif self.extension and aggregate_mutation_type == 'slot_m4':
                    # M4: planner + executor (placeholder operators)
                    result = slot_pipeline.analyze_query(query)
                    if not result.success:
                        print(f"M4槽位提取失败: {result.error}")
                        continue
                    print(f"M1槽位提取完成，槽位数量: {len(result.slots)}")

                    def _compare_hook(base_sql, mutated_sql, slot_id, operator_id):
                        if not comparator:
                            print(f"  COMPARE_PENDING slot={slot_id} op={operator_id}")
                            return
                        report = comparator.compare(base_sql, mutated_sql)
                        print(
                            "  COMPARE_RESULT "
                            f"slot={slot_id} op={operator_id} "
                            f"comparable={report.comparable} success={report.success} "
                            f"column_name_match={report.column_name_match} "
                            f"row_type_match={report.row_type_match} "
                            f"unmatched_original={report.unmatched_rows_original} "
                            f"unmatched_mutated={report.unmatched_rows_mutated} "
                            f"error={report.error}"
                        )

                    exec_results = slot_pipeline.execute_single_slot_mutations(
                        result,
                        compare_hook=_compare_hook,
                    )
                    for exec_result in exec_results:
                        print(f"M4变异完成，mutated_sql: {exec_result.mutated_sql}")
                        for r in exec_result.results:
                           print(f"  APPLY slot={r.slot_id} op={r.operator_id} applied={r.applied}")
                    
                else:
                    # 使用常规的聚合函数变异已移除
                    print("常规聚合函数变异已移除")
                # 每处理完一批查询后释放一些内存
                if processed_count % batch_size == 0:
                    batch_count += 1
                    print(f"已完成第 {batch_count} 批处理，释放部分内存...")
                    # 显式删除大对象，帮助垃圾回收
                    del ast, sql
                    # 只在定义了这些变量的情况下才删除它们
                    if 'change_ast' in locals():
                        del change_ast
                    if 'mutable_nodes' in locals():
                        del mutable_nodes
                    
            except Exception as e:
                print(f"处理查询时出错: {str(e)}")
                # 继续处理下一个查询
                continue
        
        print(f"\n预处理完成！总共处理了 {processed_count} 个种子查询")
            
    def preprocess_math_equivalence(self, ast):
        """对数学等价变异的SQL AST进行预处理
        
        参数:
        - ast: SQL的AST对象
        
        返回:
        - 处理后的AST对象
        """
        try:
            # 查找所有LIMIT子句
            for expr in ast.find_all(sqlglot.expressions.Limit):
                # 检查LIMIT是否位于Alias节点下的Subquery节点中
                parent = expr.parent
                skip_removal = False
                
                # 向上查找父节点链
                while parent:
                    # 检查是否满足Alias->Subquery的结构
                    if isinstance(parent, sqlglot.expressions.Subquery):
                        grandparent = parent.parent
                        if grandparent.__class__.__name__ in ['Alias','GT','LT','EQ','NEQ','GTE','LTE']:
                            print(f"保留Alias节点下Subquery中的LIMIT子句")
                            skip_removal = True
                            break
                    parent = parent.parent
                
                # 只有不满足特定条件时才移除LIMIT
                if not skip_removal:
                    print("删除数学等价变异中的LIMIT子句")
                    expr.replace(None)
            
        except Exception as e:
            print(f"数学等价变异预处理时出错: {str(e)}")
        
        return ast

    def _is_aggregate_function(self, node):
        """判断节点是否是聚合函数"""
        # 聚合函数类型列表，基于sqlglot实际支持的类名
        aggregate_functions = [
            'Avg', 'Count', 'Max', 'Min', 'Sum',
            'GroupConcat',
            'Std', 'Stddev', 'StddevPop', 'StddevSamp',
            'Variance', 'VariancePop', 'VarPop', 'VarSamp', 'StdDevPop', 'StdDevSamp',
            'BitAnd', 'BitOr', 'BitXor','Exp',
            
        ]

        # 检查节点类型名称是否是聚合函数
        if hasattr(node, '__class__'):
            class_name = node.__class__.__name__
            
            # 直接检查类名
            if class_name in aggregate_functions:
                return True
            
            # 特殊处理Anonymous类型的聚合函数
            if class_name == 'Anonymous':
                # 检查函数名称是否是聚合函数
                if hasattr(node, 'name') and node.name is not None:
                    func_name = node.name.upper()
                    if func_name in [
                        'STD', 'STDDEV', 'VAR', 'VARIANCE', 'BIT_AND', 'BIT_OR', 'BIT_XOR',
                        'AVG', 'COUNT', 'MAX', 'MIN', 'SUM', 'GROUP_CONCAT','EXP',
                    ]:
                        return True
                elif hasattr(node, 'this') and isinstance(node.this, str):
                    # 有些情况下this属性是字符串形式的函数名
                    func_name = node.this.upper()
                    if func_name in [
                        'STD', 'STDDEV', 'VAR', 'VARIANCE', 'BIT_AND', 'BIT_OR', 'BIT_XOR',
                        'AVG', 'COUNT', 'MAX', 'MIN', 'SUM', 'GROUP_CONCAT','EXP',
                    ]:
                        return True
        
        return False
    
    def detailsolve(self, ast):
        """
        对SQL的AST进行预处理，支持各种类型的SQL结构，包括集合操作
        重构：先通过walk获取所有select类型节点，然后逐个处理
        """
        print(f"=== 原始AST ===")
        print(ast.__class__.__name__)
        print(ast)
        try:
            # 步骤2: 获取所有select类型节点
            select_nodes = []
            for node in ast.walk():
                if isinstance(node, sqlglot.expressions.Select):
                    select_nodes.append(node)
            
            print(f"找到 {len(select_nodes)} 个SELECT节点")
            
            # 步骤3: 逐个处理SELECT节点
            for i, select_node in enumerate(select_nodes):
                print(f"处理SELECT节点 {i+1}/{len(select_nodes)}")
                self._process_select_node(select_node)
        except Exception as e:
            print(f"预处理AST时出错: {str(e)}")
        
        return ast
      

    def _process_select_node(self, ast):
        """处理单个SELECT节点"""
        print(f"=== 处理SELECT节点 ===")
        print(ast)
        
        # 处理select子句
        for i, expr in enumerate(ast.expressions):
            # 处理窗口函数
            if isinstance(expr, sqlglot.expressions.Alias) and isinstance(expr.this, sqlglot.expressions.Window):
                print(f"检测到窗口函数: {expr}")
                # 将窗口函数替换为整数值1
                new_expr = sqlglot.expressions.Alias(
                    this=sqlglot.expressions.Literal(this=1, is_string=False),
                    alias=expr.alias
                )
                ast.expressions[i].replace(new_expr)
                print(f"已替换为: {new_expr}")
            # 处理聚合函数
            elif isinstance(expr, sqlglot.expressions.Alias) and self._is_aggregate_function(expr.this):
                print(f"检测到聚合函数: {expr}")
                agg_func = expr.this
                print(agg_func.args)
                print(agg_func.__class__.__name__)
                
                # 获取聚合函数的参数
                args = []
                if agg_func.__class__.__name__ == 'GroupConcat':
                    args.append(agg_func.this.this)
                    print(args)
                elif agg_func.__class__.__name__ != 'Anonymous' and hasattr(agg_func, 'args') and 'this' in agg_func.args:
                    args = agg_func.args['this'] if isinstance(agg_func.args['this'], list) else [agg_func.args['this']]
                elif agg_func.__class__.__name__ =='Anonymous':
                    args = agg_func.expressions
                # 检查是否包含distinct
                has_distinct = False
                
                # 在sqlglot中，DISTINCT是通过在聚合函数的this属性中包含Distinct节点来表示的
                if hasattr(agg_func, 'this') and hasattr(agg_func.this, '__class__') and agg_func.this.__class__.__name__ == 'Distinct':
                    has_distinct = True
                    print(f"聚合函数包含DISTINCT关键字")
                    # 如果是DISTINCT，参数在Distinct节点的expressions中
                    if hasattr(agg_func.this, 'expressions') and agg_func.this.expressions:
                        args = agg_func.this.expressions
                
                # 如果有参数，替换为第一个参数
                if args:
                    # 保留原始别名
                    new_expr = sqlglot.expressions.Alias(
                        this=args[0],
                        alias=expr.alias
                    )
                    ast.expressions[i].replace(new_expr)
                    print(f"已将聚合函数替换为其参数: {new_expr}")
                else:
                    print("聚合函数没有参数，跳过替换")
            elif isinstance(expr, sqlglot.expressions.Alias) and isinstance(expr.this, sqlglot.expressions.Subquery):
                new_expr = sqlglot.expressions.Alias(
                    this=sqlglot.expressions.Literal(this=1, is_string=False),
                    alias=expr.alias
                )
                ast.expressions[i].replace(new_expr)
                print(f"已将子查询替换为整数值1: {new_expr}")

        # 处理limit子句
        # 通过args字典获取并删除LIMIT子句
        if hasattr(ast, 'args') and 'limit' in ast.args and ast.args['limit']:
            limit_clause = ast.args['limit']
            print(f"检测到LIMIT子句: {limit_clause}")
            # 删除LIMIT子句
            del ast.args['limit']
            print("已移除LIMIT子句")  
        
        # 处理join子句
        if hasattr(ast, 'args') and 'joins' in ast.args:
           
            joins_clause = ast.args['joins']
            print(f"检测到JOIN子句: {joins_clause}")
            if joins_clause:
                # 只处理当前JOIN子句下的直接JOIN节点，不处理更深层次的JOIN
                # 检查JOIN子句是否有expressions属性
                for clause in joins_clause:
                    if 'side' in clause.args:
                        print(clause.args['side'])
                        clause.args['side'] = None
                        # 检查是否已经是INNER JOIN (通过args字典访问join_type)
                        clause.args['kind'] = 'INNER'
                    if 'kind' in clause.args:
                        clause.args['kind'] = 'INNER'
        
        # 处理group子句 
        if hasattr(ast, 'args') and 'group' in ast.args:
            group_clause = ast.args['group']
            if group_clause:
                print(f"检测到GROUP BY子句: {group_clause}")
                # 删除GROUP BY子句
                del ast.args['group']
                print("已移除GROUP BY子句")  
        # 处理HAVING子句
        if hasattr(ast, 'args') and 'having' in ast.args and ast.args['having']:
            having_clause = ast.args['having']
            print(f"检测到HAVING子句: {having_clause}")
            
            # 检查HAVING子句的内部子节点是否包含聚合函数
            for value in having_clause.walk():
                    if isinstance(value, sqlglot.expressions.Expression) and self._is_aggregate_function(value):
                        # 找到聚合函数子节点
                        agg_func = value

                        print(agg_func.args)
                        print(agg_func.__class__.__name__)
                        
                        # 获取聚合函数的参数
                        args = []
                        if agg_func.__class__.__name__ == 'GroupConcat':
                            args.append(agg_func.this.this)
                            print(args)
                        elif agg_func.__class__.__name__ != 'Anonymous' and hasattr(agg_func, 'args') and 'this' in agg_func.args:
                            args = agg_func.args['this'] if isinstance(agg_func.args['this'], list) else [agg_func.args['this']]
                        elif agg_func.__class__.__name__ =='Anonymous':
                            args = agg_func.expressions
                        # 检查是否包含distinct
                        has_distinct = False
                        
                        # 在sqlglot中，DISTINCT是通过在聚合函数的this属性中包含Distinct节点来表示的
                        if hasattr(agg_func, 'this') and hasattr(agg_func.this, '__class__') and agg_func.this.__class__.__name__ == 'Distinct':
                            has_distinct = True
                            print(f"聚合函数包含DISTINCT关键字")
                            # 如果是DISTINCT，参数在Distinct节点的expressions中
                            if hasattr(agg_func.this, 'expressions') and agg_func.this.expressions:
                                args = agg_func.this.expressions
                        
                        # 如果有参数，替换为第一个参数
                        if args:
                            # 保留原始别名
                            new_expr = args[0]
                            agg_func.replace(new_expr)
                            print(f"已将聚合函数替换为其参数: {new_expr}")
                        else:
                            print("聚合函数没有参数，跳过替换")
        if hasattr(ast, 'args') and 'where' in ast.args and ast.args['where']:
            where_clause = ast.args['where']
            print(f"检测到WHERE子句: {where_clause}")
            
            # 使用walk方法遍历所有节点，并直接处理聚合函数
            # 将walk结果转换为列表以避免遍历过程中修改的问题
            for node in list(where_clause.walk()):
                if self._is_aggregate_function(node):
                    print(f"在WHERE子句中检测到聚合函数: {node}")
                    
                    # 获取聚合函数的参数
                    replacement = None
                    if hasattr(node, 'args') and 'this' in node.args:
                        args = node.args['this']
                        replacement = args[0] if isinstance(args, list) and args else args
                    elif hasattr(node, 'this') and node.this:
                        # 处理Distinct情况
                        if hasattr(node.this, '__class__') and node.this.__class__.__name__ == 'Distinct':
                            if hasattr(node.this, 'expressions') and node.this.expressions:
                                replacement = node.this.expressions[0] if node.this.expressions else None
                        else:
                            replacement = node.this
                    
                    # 如果有替换参数，直接替换节点
                    if replacement:
                        print(f"将聚合函数替换为其参数: {replacement}")
                        # 获取node的父节点和键名，然后进行替换
                        parent = None
                        parent_key = None
                        # 查找父节点和键名
                        for potential_parent in list(where_clause.walk()):
                            if hasattr(potential_parent, 'args'):
                                for key, value in potential_parent.args.items():
                                    if value is node:
                                        parent = potential_parent
                                        parent_key = key
                                        break
                                    elif isinstance(value, list):
                                        for i, item in enumerate(value):
                                            if item is node:
                                                parent = potential_parent
                                                parent_key = (key, i)
                                                break
                                if parent:
                                    break
                        
                        # 执行替换
                        if parent and parent_key:
                            if isinstance(parent_key, tuple):
                                # 替换列表中的元素
                                parent.args[parent_key[0]][parent_key[1]] = replacement
                            else:
                                # 替换字典中的值
                                parent.args[parent_key] = replacement
                            print(f"已成功替换聚合函数节点")
                    
        # 处理标量查询
        # 判断是否是标量查询（返回单行单列结果的查询）
        # 这里简单判断：只有一个表达式，没有GROUP BY子句，没有JOIN，可能是标量查询
        is_scalar_query = False
        if hasattr(ast, 'expressions') and len(ast.expressions) == 1:
            # 检查是否没有GROUP BY子句
            has_group_by = hasattr(ast, 'args') and 'group' in ast.args and ast.args['group']
            
            # 检查是否没有复杂的JOIN
            has_complex_join = False
            if hasattr(ast, 'args') and 'from' in ast.args and ast.args['from']:
                from_clause = ast.args['from']
                if hasattr(from_clause, 'expressions'):
                    for expr in from_clause.expressions:
                        if isinstance(expr, sqlglot.expressions.Join):
                            has_complex_join = True
                            break
            
            # 判断为标量查询
            if not has_group_by and not has_complex_join:
                is_scalar_query = True
        
        if is_scalar_query:
            print(f"检测到标量查询: {ast}")
            order_by_expr = None
            # 从SELECT表达式中获取列作为ORDER BY参数
            select_expr = ast.expressions[0]
            if isinstance(select_expr, sqlglot.expressions.Alias) and isinstance(select_expr.this, sqlglot.expressions.Column):
                order_by_expr = select_expr.this
            elif isinstance(select_expr, sqlglot.expressions.Column):
                order_by_expr = select_expr
            
            # 添加ORDER BY子句
            if hasattr(ast, 'args'):
                if order_by_expr:
                    # 创建ORDER BY表达式
                    order_by = sqlglot.expressions.Order(expressions=[order_by_expr])
                    ast.args['order'] = order_by
                    print(f"已添加ORDER BY子句: {order_by_expr}")
                
                # 添加LIMIT 1子句
                limit = sqlglot.expressions.Limit(expression='1')
                ast.args['limit'] = limit
                print("已添加LIMIT 1子句")
        print(f"修改后的AST: {ast}")

        
        
        
        
    
    
