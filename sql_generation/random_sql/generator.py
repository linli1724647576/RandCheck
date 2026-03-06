import os
import random
import re
import string
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from ast_nodes import (
    ASTNode,
    ArithmeticNode,
    CaseNode,
    ColumnReferenceNode,
    ComparisonNode,
    FromNode,
    FunctionCallNode,
    GroupByNode,
    LiteralNode,
    LogicalNode,
    OrderByNode,
    SetOperationNode,
    SubqueryNode,
    WithNode,
    LimitNode,
    SelectNode,
)

from data_structures.column import Column
from data_structures.function import Function
from data_structures.table import Table
from data_structures.dependency import Dependency
from data_structures.db_dialect import DBDialect, get_current_dialect, get_dialect_config, set_dialect

from sql_generation.random_sql import state as _state
from sql_generation.random_sql.column_tracker import ColumnUsageTracker, get_random_column_with_tracker
from sql_generation.random_sql.ddl_dml import generate_create_table_sql, generate_insert_sql
from sql_generation.random_sql.expressions import (
    create_expression_of_type,
    create_random_expression,
    ensure_boolean_expression,
)
from sql_generation.random_sql.joins import create_join_condition, generate_table_alias
from sql_generation.random_sql.predicates import create_where_condition
from sql_generation.random_sql.subqueries import create_select_subquery
from sql_generation.random_sql.samples import create_sample_functions, create_sample_tables
from sql_generation.random_sql.geometry import (
    create_geojson_literal_node,
    create_geohash_literal_node,
    create_geometry_literal_node,
    create_geometry_wkt,
    create_geometry_wkt_for_function,
    create_wkb_literal_node,
    create_wkt_literal_node,
    get_geometry_type_for_function,
    is_geojson_function,
    is_geohash_function,
    is_geometry_type,
    is_wkt_function,
)
from sql_generation.random_sql.io_utils import generate_index_sqls, save_sql_to_file
from sql_generation.random_sql.type_utils import (
    ORDERABLE_CATEGORIES,
    adjust_expected_type_for_conditionals,
    adjust_expected_type_for_min_max,
    get_cast_types,
    get_comparison_operators,
    get_full_column_identifier,
    get_safe_comparison_category,
    map_param_type_to_category,
    map_return_type_to_category,
    normalize_category,
)


def get_tables():
    return _state.get_tables()


def set_tables(tables_list):
    _state.set_tables(tables_list)


def _get_subquery_depth() -> int:
    return _state.get_subquery_depth()


def _set_subquery_depth(depth: int) -> None:
    _state.set_subquery_depth(depth)


def _collect_column_refs(node: ASTNode) -> List[ColumnReferenceNode]:
    if node is None:
        return []
    if isinstance(node, ColumnReferenceNode):
        return [node]
    refs: List[ColumnReferenceNode] = []
    if hasattr(node, "children"):
        for child in node.children:
            refs.extend(_collect_column_refs(child))
    return refs


def _add_group_by_from_scalar(group_by: GroupByNode, expr: ASTNode) -> None:
    refs = _collect_column_refs(expr)
    for ref in refs:
        group_by.add_expression(ref)

def _matches_required_geometry(col: Column, required: Optional[str]) -> bool:
    if not required:
        return True
    data_type = str(getattr(col, "data_type", "")).upper()
    if required in data_type:
        return True
    return data_type == "GEOMETRY"

def generate_random_sql(tables: List[Table], functions: List[Function], current_depth: int = 0) -> str:
    """鐢熸垚闅忔満SQL鏌ヨ"""
    use_cte = False
    # 鑾峰彇褰撳墠鏁版嵁搴撴柟瑷€
    current_dialect = get_current_dialect()
    # 妫€鏌ユ槸鍚︽槸Percona鏂硅█
    # 鐜板湪鍏佽Percona浣跨敤WITH鏌ヨ鍔熻兘
    is_percona = current_dialect and 'PerconaDialect' == current_dialect.__class__.__name__
    # 棣栧厛灏濊瘯鐢熸垚CTE鏌ヨ锛屽寘鎷琍ercona鏂硅█
    if random.random() > 0.3:  # 鎵€鏈夋敮鎸乄ITH瀛愬彞鐨勬柟瑷€锛屽寘鎷琍ercona锛岄兘鏈夌浉鍚屾鐜囩敓鎴怶ITH绫诲瀷鏌ヨ
        use_cte = True
        with_node = WithNode()
        
        # 鐢熸垚1-2涓狢TE
        num_ctes = random.randint(1, 2)
        for i in range(num_ctes):
            cte_name = f"cte_{random.randint(1, 999)}"
            cte_query = SelectNode()
            # 鍒濆鍖朇TE鏌ヨ鐨勫垪浣跨敤璺熻釜鍣?
            cte_column_tracker = ColumnUsageTracker()
            cte_query.tables = tables
            cte_query.functions = functions
            # 瀛樺偍鍒楄窡韪櫒鐢ㄤ簬鍚庣画浣跨敤
            cte_query.metadata = {'column_tracker': cte_column_tracker}
            
            # 涓篊TE閫夋嫨涓€涓〃
            cte_table = random.choice(tables)
            cte_alias = generate_table_alias()
            
            # 鍒涘缓FROM瀛愬彞
            cte_from = FromNode()
            cte_from.add_table(cte_table, cte_alias)
            cte_query.set_from_clause(cte_from)
            # 鍒濆鍖朇TE鏌ヨ鐨勫垪杩借釜鍣ㄧ殑鍙敤鍒椾俊鎭?
            if cte_query.metadata and 'column_tracker' in cte_query.metadata:
                cte_column_tracker = cte_query.metadata['column_tracker']
                if hasattr(cte_column_tracker, 'initialize_from_from_node'):
                    cte_column_tracker.initialize_from_from_node(cte_from)
            
            # 娣诲姞SELECT琛ㄨ揪寮?
            num_columns = random.randint(2, 4)
            non_aggregate_columns = []  # 瀛樺偍闈炶仛鍚堝垪
            has_aggregate_function = False  # 璺熻釜鏄惁鍖呭惈鑱氬悎鍑芥暟
            for j in range(num_columns):
                # 闅忔満娣诲姞
                expr = create_random_expression([cte_table], functions, cte_from, cte_table, cte_alias, use_subquery=False,column_tracker=cte_column_tracker, for_select=True)
                cte_query.add_select_expression(expr, f"col_{j+1}")
                
            # 鏂扮殑GROUP BY瀛愬彞娣诲姞閫昏緫锛氬厛鐢熸垚鎵€鏈夎〃杈惧紡锛岀劧鍚庡垽鏂鐞?
            # 鍒涘缓GroupByNode
            cte_group_by = GroupByNode()
            # 妫€鏌ユ槸鍚﹀寘鍚仛鍚堝嚱鏁?
            has_aggregate = False
            
            if hasattr(cte_query, 'select_expressions'):
                try:
                    # 灏濊瘯閬嶅巻select_expressions
                    for expr, alias in cte_query.select_expressions:
                        # 妫€鏌ヨ〃杈惧紡鏄惁鏈塮unction灞炴€?
                        if hasattr(expr, 'function'):
                            if getattr(expr.function, 'func_type', '') == 'aggregate':
                                has_aggregate = True
                except Exception as e:
                    print(f"閬嶅巻cte_query.select_expressions鏃跺嚭閿? {e}")
            
            # 濡傛灉鏈夎仛鍚堝嚱鏁帮紝鍒欐坊鍔燝ROUP BY瀛愬彞
            if has_aggregate:
                # 鏀堕泦鎵€鏈夐潪鑱氬悎鍒楀苟娣诲姞鍒癎ROUP BY
                for expr, alias in cte_query.select_expressions:
                    if not (hasattr(expr, 'function') and getattr(expr.function, 'func_type', '') == 'aggregate'):
                        # 鍒ゆ柇鏄惁涓烘爣閲忓嚱鏁板垪鎴栧垪寮曠敤
                        if hasattr(expr, 'function') and getattr(expr.function, 'func_type', '') == 'scalar':
                            _add_group_by_from_scalar(cte_group_by, expr)
                        elif type(expr).__name__ == 'ColumnReferenceNode':
                            cte_group_by.add_expression(expr)
                        # 鍒ゆ柇绐楀彛鍑芥暟
                        elif hasattr(expr, 'function') and getattr(expr.function, 'func_type', '') == 'window':
                            if hasattr(expr, 'children'):
                                    for arg in expr.children:
                                        # 濡傛灉鍙傛暟鏄垪寮曠敤鎴栨爣閲忓嚱鏁帮紝娣诲姞鍒癎ROUP BY
                                        if type(arg).__name__ == 'ColumnReferenceNode':
                                            cte_group_by.add_expression(arg)
                                        elif type(arg).__name__ == 'FunctionCallNode' and hasattr(arg.function, 'func_type') and arg.function.func_type == 'scalar':
                                            for child in arg.children:
                                                if type(child).__name__ == 'ColumnReferenceNode':
                                                    cte_group_by.add_expression(child)
                                            
                            if hasattr(expr, 'metadata'):
                                # 澶勭悊partition_by
                                if expr.metadata.get('partition_by'):
                                    partition_by = expr.metadata.get('partition_by')
                                    # 灏濊瘯鑾峰彇cte_from锛堝亣璁惧畠鍦ㄧ埗浣滅敤鍩熶腑鍙敤锛?
                                    if 'cte_from' in locals() or 'cte_from' in globals():
                                        available_from_node = locals().get('cte_from') or globals().get('cte_from')
                                        
                                        for part_expr in partition_by:
                                            try:
                                                # 瑙ｆ瀽琛ㄥ埆鍚嶅拰鍒楀悕
                                                if '.' in part_expr:
                                                    alias_part, col_part = part_expr.split('.', 1)
                                                    # 娓呯悊鍙兘鐨勫紩鍙?
                                                    alias_part = alias_part.strip('"\'')
                                                    col_part = col_part.strip('"\'')
                                                    
                                                    # 鑾峰彇琛ㄥ璞?
                                                    table_ref = available_from_node.get_table_for_alias(alias_part)
                                                    if table_ref and hasattr(table_ref, 'get_column'):
                                                        # 鑾峰彇鍒楀璞?
                                                        col = table_ref.get_column(col_part)
                                                        if col:
                                                            # 鍒涘缓ColumnReferenceNode瀵硅薄
                                                            col_ref = ColumnReferenceNode(col, alias_part)
                                                            # 娣诲姞鍒癎ROUP BY
                                                            cte_group_by.add_expression(col_ref)
                                            except Exception as e:
                                                print(f"  杞崲partition_by琛ㄨ揪寮忔椂鍑洪敊: {e}")
                                
                                # 澶勭悊order_by
                                if expr.metadata.get('order_by'):
                                    order_by = expr.metadata.get('order_by')
                                    # 灏濊瘯鑾峰彇cte_from锛堝亣璁惧畠鍦ㄧ埗浣滅敤鍩熶腑鍙敤锛?
                                    if 'cte_from' in locals() or 'cte_from' in globals():
                                        available_from_node = locals().get('cte_from') or globals().get('cte_from')
                                        
                                        # 澶勭悊order_by琛ㄨ揪寮?
                                        main_expr = []
                                        for order in order_by:
                                            expr_parts = order.rsplit(' ', 1)
                                            if len(expr_parts) == 2 and expr_parts[1].upper() in ['ASC', 'DESC']:
                                                main_expr.append(expr_parts[0])
                                            else:
                                                main_expr.append(order)
                                        
                                        for part_expr in main_expr:
                                            try:
                                                # 瑙ｆ瀽琛ㄥ埆鍚嶅拰鍒楀悕
                                                if '.' in part_expr:
                                                    alias_part, col_part = part_expr.split('.', 1)
                                                    # 娓呯悊鍙兘鐨勫紩鍙?
                                                    alias_part = alias_part.strip('"\'')
                                                    col_part = col_part.strip('"\'')
                                                    
                                                    # 鑾峰彇琛ㄥ璞?
                                                    table_ref = available_from_node.get_table_for_alias(alias_part)
                                                    if table_ref and hasattr(table_ref, 'get_column'):
                                                        # 鑾峰彇鍒楀璞?
                                                        col = table_ref.get_column(col_part)
                                                        if col:
                                                            # 鍒涘缓ColumnReferenceNode瀵硅薄
                                                            col_ref = ColumnReferenceNode(col, alias_part)
                                                            # 娣诲姞鍒癎ROUP BY
                                                            cte_group_by.add_expression(col_ref)
                                            except Exception as e:
                                                print(f"  杞崲order_by琛ㄨ揪寮忔椂鍑洪敊: {e}")
                
                # 璁剧疆GROUP BY瀛愬彞
                if cte_group_by.expressions:
                    cte_query.set_group_by_clause(cte_group_by)
            
            # 娣诲姞WHERE鏉′欢
            if random.random() > 0.5:
                # 纭繚WHERE鏉′欢鍙紩鐢–TE涓疄闄呭瓨鍦ㄧ殑鍒?
                # 纭繚column_tracker瀛樺湪锛堜箣鍓嶅凡鍒濆鍖栵級
                cte_column_tracker = cte_query.metadata.get('column_tracker')
                where = create_where_condition([cte_table], functions, cte_from, cte_table, cte_alias, use_subquery=False, column_tracker=cte_column_tracker)
                cte_query.set_where_clause(where)
                
                # 楠岃瘉骞朵慨澶峎HERE鏉′欢涓殑鍒楀紩鐢?
                if hasattr(cte_query, 'validate_all_columns'):
                    valid, errors = cte_query.validate_all_columns()
                    if not valid and hasattr(cte_query, 'repair_invalid_columns'):
                        cte_query.repair_invalid_columns()
            
            # 娣诲姞CTE鍒癢ITH瀛愬彞
            with_node.add_cte(cte_name, cte_query, num_columns)
        
        # 鐢熸垚涓绘煡璇紝浣跨敤CTE鍜屽疄闄呰〃
        main_query = SelectNode()
        main_query.tables = tables
        main_query.functions = functions
        
        main_from = FromNode()
        
        # 鍒涘缓铏氭嫙琛ㄨ〃绀烘墍鏈塁TE
        cte_tables = []
        for cte_name, cte_query, cte_num_columns in with_node.ctes:
            # 鍒涘缓铏氭嫙琛?
            # 灏濊瘯浠巆te_query涓彁鍙栧疄闄呯殑鍒椾俊鎭?
            columns = []
            
            # 妫€鏌te_query鏄惁鏈塻elect_expressions灞炴€?
            if hasattr(cte_query, 'select_expressions') and cte_query.select_expressions:
                # 閬嶅巻select_expressions锛屾彁鍙栧垪淇℃伅
                for j, (expr, alias) in enumerate(cte_query.select_expressions):
                    # 榛樿鍊?
                    col_name = alias or f"col_{j+1}"
                    data_type = "INT"
                    category = "numeric"
                    is_nullable = False
                    
                    # 灏濊瘯浠庤〃杈惧紡涓幏鍙栧疄闄呯被鍨嬩俊鎭?
                    if hasattr(expr, 'metadata') and expr.metadata:
                        if 'data_type' in expr.metadata:
                            data_type = expr.metadata['data_type']
                        if 'category' in expr.metadata:
                            category = expr.metadata['category']
                        elif data_type:
                            category = map_return_type_to_category(data_type)
                        if 'is_nullable' in expr.metadata:
                            is_nullable = expr.metadata['is_nullable']
                    elif isinstance(expr, SubqueryNode):
                        if expr.column_alias_map:
                            _, data_type, category = next(iter(expr.column_alias_map.values()))
                        else:
                            data_type = "INT"
                            category = "numeric"
                    elif hasattr(expr, 'column'):
                        # 濡傛灉鏄垪寮曠敤锛屼娇鐢ㄥ師濮嬪垪鐨勪俊鎭?
                        column = expr.column
                        if hasattr(column, 'data_type'):
                            data_type = column.data_type
                        if hasattr(column, 'category'):
                            category = column.category
                        if hasattr(column, 'is_nullable'):
                            is_nullable = column.is_nullable
                    elif hasattr(expr, 'function'):
                        # 濡傛灉鏄嚱鏁拌皟鐢紝鏍规嵁鍑芥暟绫诲瀷鎺ㄦ柇
                        func = expr.function
                        if hasattr(func, 'return_type'):
                            data_type = func.return_type
                        if hasattr(func, 'return_category'):
                            category = func.return_category
                        elif data_type:
                            category = map_return_type_to_category(data_type)
                    
                    # 鍒涘缓鍒?
                    columns.append(Column(
                        name=col_name,
                        data_type=data_type,
                        category=category,
                        is_nullable=is_nullable,
                        table_name=cte_name
                    ))
            else:
                # 濡傛灉鏃犳硶鑾峰彇瀹為檯淇℃伅锛屽洖閫€鍒伴粯璁ゆ柟寮?
                columns = [Column(f"col_{j+1}", "INT", "numeric", False, cte_name) for j in range(cte_num_columns)]
            
            # 鍒涘缓CTE铏氭嫙琛?
            cte_table = Table(
                name=cte_name,
                columns=columns,
                primary_key=columns[0].name if columns else "col_1",
                foreign_keys=[]
            )
            # 娣诲姞column_alias_map灞炴€э紝浠ヤ究鍒楄拷韪櫒鑳藉姝ｇ‘璇嗗埆鍜岃窡韪繖浜涜櫄鎷熻〃鐨勫垪
            cte_table.column_alias_map = {}
            for col in columns:
                cte_table.column_alias_map[col.name] = (col.name, col.data_type, col.category)
            cte_tables.append(cte_table)
        
        # 鍚堝苟CTE铏氭嫙琛ㄥ拰鍘熷琛?
        combined_tables = tables + cte_tables
        tables = combined_tables
        
    
    # 闅忔満鍐冲畾鏄惁鐢熸垚闆嗗悎鎿嶄綔
    if random.random() > 0.7 and current_depth == 0:  
        # 30% 姒傜巼鐢熸垚闆嗗悎鎿嶄綔锛屼笖鍙湪椤跺眰鏌ヨ鐢熸垚
        # 閫夋嫨闆嗗悎鎿嶄綔绫诲瀷锛屽寘鎷琁NTERSECT鍜孍XCEPT
        dialect = get_current_dialect()
        operation_types = ['UNION', 'UNION ALL']
        if hasattr(dialect, 'supports_intersect_operator'):
            if dialect.supports_intersect_operator():
                operation_types.append('INTERSECT')
        elif dialect.name == 'POSTGRESQL':
            operation_types.append('INTERSECT')
        if hasattr(dialect, 'supports_except_operator'):
            if dialect.supports_except_operator():
                operation_types.append('EXCEPT')
        elif dialect.name == 'POSTGRESQL':
            operation_types.append('EXCEPT')
        operation_type = random.choice(operation_types)
        
        # 鍒涘缓闆嗗悎鎿嶄綔鑺傜偣
        set_op_node = SetOperationNode(operation_type)
        
        # 鐢熸垚2-3涓弬涓庨泦鍚堟搷浣滅殑鏌ヨ
        num_queries = random.randint(2, 3)
        for i in range(num_queries):
            # 鐢熸垚涓€涓煡璇紙闄愬埗娣卞害锛岄伩鍏嶈繃澶嶆潅锛?
            select_node = SelectNode()
            select_node.tables = tables
            select_node.functions = functions
            # 鍒濆鍖栧垪浣跨敤璺熻釜鍣?
            column_tracker = ColumnUsageTracker()
            select_node.metadata = {'column_tracker': column_tracker}
            
            # 闅忔満鍐冲畾鏄惁浣跨敤DISTINCT
            if random.random() > 0.8:
                select_node.distinct = True
            
            # 閫夋嫨涓昏〃
            main_table = random.choice(tables)
            main_alias = generate_table_alias()
            
            # 鍒涘缓FROM瀛愬彞
            from_node = FromNode()
            from_node.add_table(main_table, main_alias)
            select_node.set_from_clause(from_node)
            # 鍒濆鍖栧垪杩借釜鍣ㄧ殑鍙敤鍒椾俊鎭?
            
            
            # 闅忔満娣诲姞杩炴帴琛紙绠€鍖栵紝涓嶈秴杩?涓繛鎺ワ級
            has_join = False
            join_table = None
            join_alias = None
            if random.random() > 0.5 and i == 0:  # 绗竴涓煡璇㈠彲鑳芥湁杩炴帴锛屽叾浠栨煡璇㈠敖閲忕畝鍗?
                # 閫夋嫨涓€涓笉鍚岀殑琛ㄨ繘琛岃繛鎺?
                available_tables = [t for t in tables if t.name != main_table.name]
                if available_tables:
                    join_table = random.choice(available_tables)
                    join_alias = generate_table_alias()
                    
                    # 闅忔満閫夋嫨杩炴帴绫诲瀷
                    join_type = random.choice(['INNER', 'LEFT','RIGHT','CROSS'])
                    # 灏濊瘯鍒涘缓鍚堢悊鐨勮繛鎺ユ潯浠?
                    join_condition = create_join_condition(main_table, main_alias, join_table, join_alias)
                    from_node.add_join(join_type, join_table, join_alias, join_condition)
                    has_join = True
                    select_node.set_from_clause(from_node)
            if select_node.metadata and 'column_tracker' in select_node.metadata:
                column_tracker = select_node.metadata['column_tracker']
                if hasattr(column_tracker, 'initialize_from_from_node'):
                    column_tracker.initialize_from_from_node(from_node)
            # 鐢熸垚SELECT瀛愬彞 - 纭繚鎵€鏈夋煡璇㈢殑鍒楁暟鍜岀被鍨嬪吋瀹?
            if i == 0:  # 绗竴涓煡璇㈠喅瀹氬垪鏁板拰鍒楃被鍨?
                num_columns = 2 + random.randint(0, 2)  # 2-4鍒?
                for j in range(num_columns):
                    expr_node = create_random_expression(
                        tables, functions, from_node, main_table, main_alias, 
                        join_table if has_join else None, join_alias if has_join else None, 
                        use_subquery=False,  # 闆嗗悎鎿嶄綔鍐呬笉浣跨敤瀛愭煡璇?
                        column_tracker=column_tracker,
                        for_select=True
                    )
                    
                    # 鐢熸垚鍒悕
                    alias = f"col_{j+1}"  # 浣跨敤缁熶竴鐨勫埆鍚嶏紝渚夸簬闆嗗悎鎿嶄綔
                    select_node.add_select_expression(expr_node, alias)
            else:  # 鍚庣画鏌ヨ闇€瑕佷笌绗竴涓煡璇㈢殑鍒楁暟鍜屽垪绫诲瀷瀹屽叏鍖归厤
                first_query = set_op_node.queries[0]
                # 纭繚鍒楁暟涓庣涓€涓煡璇㈢浉鍚?
                for j in range(len(first_query.select_expressions)):
                    # 浠庣涓€涓煡璇㈣幏鍙栧垪淇℃伅
                    first_expr, _ = first_query.select_expressions[j]
                    expr_type = first_expr.metadata.get('category', 'any')
                    
                    # 鍒涘缓鐩稿悓绫诲瀷鐨勮〃杈惧紡
                    expr_node = create_expression_of_type(
                        expr_type, tables, functions, from_node, main_table, main_alias,
                        join_table if has_join else None, join_alias if has_join else None,
                        column_tracker= column_tracker
                    )
                    
                    # 浣跨敤缁熶竴鐨勫埆鍚?
                    alias = f"col_{j+1}"
                    select_node.add_select_expression(expr_node, alias)
            
            # 鐢熸垚WHERE瀛愬彞锛堢畝鍖栵紝閬垮厤澶鏉傦級
            if random.random() > 0.6:
                # 浠巗elect_node涓幏鍙朿olumn_tracker锛堝鏋滃瓨鍦級
                select_column_tracker = select_node.metadata.get('column_tracker') if hasattr(select_node, 'metadata') else None
                where_node = create_where_condition(
                    tables, functions, from_node, main_table, main_alias,
                    join_table if has_join else None, join_alias if has_join else None,
                    use_subquery=False,  # 闆嗗悎鎿嶄綔鍐呬笉浣跨敤瀛愭煡璇?
                    column_tracker=select_column_tracker
                )
                select_node.set_where_clause(where_node)
            
            # 浠呭湪绗竴涓煡璇㈡坊鍔燨RDER BY鍜孡IMIT锛堥泦鍚堟搷浣滅殑ORDER BY鍜孡IMIT閫氬父搴旂敤浜庢渶鍚庣粨鏋滐級
            if i == 0:
                # 闅忔満娣诲姞ORDER BY瀛愬彞
                if random.random() > 0.7:
                    order_by = OrderByNode()
                    # 閫夋嫨瑕佹帓搴忕殑鍒?
                    col = main_table.get_random_column()
                    col_ref = ColumnReferenceNode(col, main_alias)
                    
                    # 妫€鏌ユ槸鍚︿娇鐢ㄤ簡DISTINCT锛屽鏋滄槸锛屽垯纭繚ORDER BY鐨勫垪鍦⊿ELECT鍒楄〃涓?
                    if select_node.distinct:
                        # 妫€鏌ユ槸鍚﹀凡鍦⊿ELECT鍒楄〃涓?
                        in_select = False
                        for selected_expr, selected_alias in select_node.select_expressions:
                            if hasattr(selected_expr, 'to_sql') and selected_expr.to_sql() == col_ref.to_sql():
                                in_select = True
                                break
                        
                        # 濡傛灉涓嶅湪SELECT鍒楄〃涓紝鍒欐坊鍔?
                        if not in_select:
                            select_node.add_select_expression(col_ref, col.name)
                    
                    order_by.add_expression(col_ref, random.choice(['ASC', 'DESC']))
                    select_node.set_order_by_clause(order_by)
                
                # 闅忔満娣诲姞LIMIT瀛愬彞
                if random.random() > 0.7:
                    select_node.set_limit_clause(LimitNode(random.randint(1, 5)))
            # 妫€鏌ユ瘡涓瓙鏌ヨ鏄惁鏈夎仛鍚堝嚱鏁板苟娣诲姞GROUP BY瀛愬彞
            # 淇濆瓨澶栭儴select_node寮曠敤
            outer_select_node = select_node
            for i, expr_item in enumerate(outer_select_node.select_expressions):
                group_by = GroupByNode()
                # 妫€鏌ユ槸鍚﹀寘鍚仛鍚堝嚱鏁?
                has_aggregate = False
                try:
                    expr, alias = expr_item
                    if hasattr(expr, 'function') and hasattr(expr.function, 'func_type') and expr.function.func_type == 'aggregate':
                        has_aggregate = True
                        
                except (TypeError, ValueError):
                    pass
                if hasattr(select_node, 'select_expressions'):
                    try:
                        # 灏濊瘯閬嶅巻select_expressions
                        expressions_count = 0
                        for expr, alias in select_node.select_expressions:
                            expressions_count += 1
                            # 妫€鏌ヨ〃杈惧紡鏄惁鏈塮unction灞炴€?
                            if hasattr(expr, 'function'):
                                if getattr(expr.function, 'func_type', '') == 'aggregate':
                                    has_aggregate = True
                    except Exception as e:
                        print(f"閬嶅巻select_expressions鏃跺嚭閿? {e}")
                else:
                    print("Warning: select_node missing select_expressions")
                # 濡傛灉鏈夎仛鍚堝嚱鏁帮紝鍒欐坊鍔燝ROUP BY瀛愬彞
                if has_aggregate:
                    added_group_columns = set()
                    # 鏀堕泦鎵€鏈夐潪鑱氬悎鍒楀苟娣诲姞鍒癎ROUP BY
                    for expr, alias in select_node.select_expressions:
                        if not (hasattr(expr, 'function') and expr.function.func_type == 'aggregate'):
                            # 鍒ゆ柇鏄惁涓烘爣閲忓嚱鏁板垪
                            if hasattr(expr, 'function') and expr.function.func_type == 'scalar':
                                    # 璁板綍娣诲姞鍓岹ROUP BY琛ㄨ揪寮忔暟閲?                                    before_count = len(group_by.expressions)
                                    _add_group_by_from_scalar(group_by, expr)
                                    # 璁板綍娣诲姞鍚嶨ROUP BY琛ㄨ揪寮忔暟閲?                                    after_count = len(group_by.expressions)
                            elif type(expr).__name__=='ColumnReferenceNode':
                                    # 璁板綍娣诲姞鍓岹ROUP BY琛ㄨ揪寮忔暟閲?                                    before_count = len(group_by.expressions)
                                    # 鐩存帴娣诲姞琛ㄨ揪寮忓埌GROUP BY
                                    group_by.add_expression(expr)
                                    # 璁板綍娣诲姞鍚嶨ROUP BY琛ㄨ揪寮忔暟閲?                                    after_count = len(group_by.expressions)
                            # 鍒ゆ柇绐楀彛鍑芥暟
                            if hasattr(expr, 'function') and expr.function.func_type == 'window':
                                # 澶勭悊绐楀彛鍑芥暟鐨勫弬鏁帮紝娣诲姞鍒癎ROUP BY
                                before_count = len(group_by.expressions)
                                if hasattr(expr, 'children'):
                                    for arg in expr.children:
                                        # 濡傛灉鍙傛暟鏄垪寮曠敤鎴栨爣閲忓嚱鏁帮紝娣诲姞鍒癎ROUP BY
                                        if type(arg).__name__ == 'ColumnReferenceNode':
                                            group_by.add_expression(arg)
                                        elif type(arg).__name__ == 'FunctionCallNode' and hasattr(arg.function, 'func_type') and arg.function.func_type == 'scalar':
                                            for child_arg in arg.children:
                                                if type(child_arg).__name__ == 'ColumnReferenceNode':
                                                    group_by.add_expression(child_arg)
                                after_count = len(group_by.expressions)
                                if hasattr(expr,'metadata'):
                                    if expr.metadata.get('partition_by'):
                                        partition_by=expr.metadata.get('partition_by')
                                        before_count = len(group_by.expressions)
                                        
                                        # 灏濊瘯鑾峰彇from_node锛堝亣璁惧畠鍦ㄧ埗浣滅敤鍩熶腑鍙敤锛?
                                        if 'from_node' in locals() or 'from_node' in globals():
                                            available_from_node = locals().get('from_node') or globals().get('from_node')
                                            
                                            for part_expr in partition_by:
                                                try:
                                                    # 瑙ｆ瀽琛ㄥ埆鍚嶅拰鍒楀悕 (鏍煎紡: table_alias.column_name)
                                                    if '.' in part_expr:
                                                        alias_part, col_part = part_expr.split('.', 1)
                                                        # 娓呯悊鍙兘鐨勫紩鍙?
                                                        alias_part = alias_part.strip('"\'')
                                                        col_part = col_part.strip('"\'')
                                                        
                                                        # 鑾峰彇琛ㄥ璞?
                                                        table_ref = available_from_node.get_table_for_alias(alias_part)
                                                        if table_ref and hasattr(table_ref, 'get_column'):
                                                            # 鑾峰彇鍒楀璞?
                                                            col = table_ref.get_column(col_part)
                                                            if col:
                                                                # 鍒涘缓ColumnReferenceNode瀵硅薄
                                                                col_ref = ColumnReferenceNode(col, alias_part)
                                                                # 娣诲姞鍒癎ROUP BY
                                                                group_by.add_expression(col_ref)
                                                                
                                                except Exception as e:
                                                    print(f"  杞崲partition_by琛ㄨ揪寮忔椂鍑洪敊: {e}")
                                        
                                        after_count = len(group_by.expressions)
                                    if expr.metadata.get('order_by'):
                                        order_by=expr.metadata.get('order_by')
                                        before_count = len(group_by.expressions)
                                        for order in order_by:
                                            expr_parts = order.rsplit(' ', 1)
                                        if len(expr_parts) == 2 and expr_parts[1].upper() in ['ASC', 'DESC']:
                                            main_expr = expr_parts[0]
                                            main_expr = [main_expr]
                                            sort_direction = expr_parts[1]
                                        else:
                                            main_expr = order_by
                                            sort_direction = None
                                        # 灏濊瘯鑾峰彇from_node锛堝亣璁惧畠鍦ㄧ埗浣滅敤鍩熶腑鍙敤锛?
                                        if 'from_node' in locals() or 'from_node' in globals():
                                            available_from_node = locals().get('from_node') or globals().get('from_node')
                                            
                                            for part_expr in main_expr:
                                                try:
                                                    # 瑙ｆ瀽琛ㄥ埆鍚嶅拰鍒楀悕 (鏍煎紡: table_alias.column_name)
                                                    if '.' in part_expr:
                                                        alias_part, col_part = part_expr.split('.', 1)
                                                        # 娓呯悊鍙兘鐨勫紩鍙?
                                                        alias_part = alias_part.strip('"\'')
                                                        col_part = col_part.strip('"\'')
                                                        
                                                        # 鑾峰彇琛ㄥ璞?
                                                        table_ref = available_from_node.get_table_for_alias(alias_part)
                                                        if table_ref and hasattr(table_ref, 'get_column'):
                                                            # 鑾峰彇鍒楀璞?
                                                            col = table_ref.get_column(col_part)
                                                            if col:
                                                                # 鍒涘缓ColumnReferenceNode瀵硅薄
                                                                col_ref = ColumnReferenceNode(col, alias_part)
                                                                # 娣诲姞鍒癎ROUP BY
                                                                group_by.add_expression(col_ref)
                                                    else:
                                                        print(f"  璀﹀憡: 鏃犳硶瑙ｆ瀽order_by琛ㄨ揪寮忔牸寮? {part_expr}")
                                                except Exception as e:
                                                    print(f"  杞崲order_by琛ㄨ揪寮忔椂鍑洪敊: {e}")
                                        else:
                                            print("  璀﹀憡: from_node瀵硅薄涓嶅彲鐢紝鏃犳硶灏唎rder_by杞崲涓篊olumnReferenceNode")
                                        
                                        after_count = len(group_by.expressions)
                            

                            # 妫€鏌elect_node鏄惁鏈塷rder_by_clause灞炴€?
                            if hasattr(select_node, 'order_by_clause') and select_node.order_by_clause:
                                # 閬嶅巻order_by_clause涓殑琛ㄨ揪寮?
                                for expr, direction in select_node.order_by_clause.expressions:
                                    expr=[expr.to_sql()]
                                    # 鍙互鍦ㄨ繖閲屾牴鎹渶瑕佸鐞嗚繖浜涜〃杈惧紡
                                    if 'from_node' in locals() or 'from_node' in globals():
                                            available_from_node = locals().get('from_node') or globals().get('from_node')
                                            for part_expr in expr:
                                                try:
                                                    # 瑙ｆ瀽琛ㄥ埆鍚嶅拰鍒楀悕 (鏍煎紡: table_alias.column_name)
                                                    if '.' in part_expr:
                                                        alias_part, col_part = part_expr.split('.', 1)
                                                        # 娓呯悊鍙兘鐨勫紩鍙?
                                                        alias_part = alias_part.strip('"\'')
                                                        col_part = col_part.strip('"\'')
                                                        
                                                        # 鑾峰彇琛ㄥ璞?
                                                        table_ref = available_from_node.get_table_for_alias(alias_part)
                                                        if table_ref and hasattr(table_ref, 'get_column'):
                                                            # 鑾峰彇鍒楀璞?
                                                            col = table_ref.get_column(col_part)
                                                            if col:
                                                                # 鍒涘缓ColumnReferenceNode瀵硅薄
                                                                col_ref = ColumnReferenceNode(col, alias_part)
                                                                # 娣诲姞鍒癎ROUP BY
                                                                group_by.add_expression(col_ref)
                                                    else:
                                                        print(f"  璀﹀憡: 鏃犳硶瑙ｆ瀽order_by琛ㄨ揪寮忔牸寮? {part_expr}")
                                                except Exception as e:
                                                    print(f"  杞崲order_by琛ㄨ揪寮忔椂鍑洪敊: {e}")
                                    else:
                                            print("  璀﹀憡: from_node瀵硅薄涓嶅彲鐢紝鏃犳硶灏唎rder_by杞崲涓篊olumnReferenceNode")
                                        
            # 璁剧疆GROUP BY瀛愬彞
            if group_by.expressions:
                select_node.group_by_clause = group_by
            # 娣诲姞鏌ヨ鍒伴泦鍚堟搷浣?
            set_op_node.add_query(select_node)
    
        # 杩斿洖闆嗗悎鎿嶄綔SQL
        if use_cte:
            return f'{with_node.to_sql()} {set_op_node.to_sql()}'
        else:
            return set_op_node.to_sql()
    
    # 鍒涘缓SELECT鑺傜偣
    select_node = SelectNode()
    # 鍒濆鍖栧垪浣跨敤璺熻釜鍣?
    column_tracker = ColumnUsageTracker()
    # 闅忔満璁剧疆distinct灞炴€э紝50%鐨勬鐜囦娇鐢―ISTINCT
    if random.random() < 0.5:
        select_node.distinct = True
    select_node.tables = tables
    select_node.functions = functions
    # 瀛樺偍鍒楄窡韪櫒鐢ㄤ簬鍚庣画浣跨敤
    select_node.metadata = {'column_tracker': column_tracker}

    # 鍒涘缓FROM瀛愬彞
    from_node = FromNode()
    sql_keywords = {'use', 'select', 'from', 'where', 'group', 'by', 'order', 'limit', 'join', 'on', 'as'}


    # 鏍规嵁褰撳墠娣卞害鍜屾渶澶ф繁搴﹀喅瀹氭槸鍚︿娇鐢ㄥ瓙鏌ヨ
    use_subquery = current_depth < _get_subquery_depth() and random.random() > 0.3
    main_alias = ''
    main_table = None

    if use_subquery and len(tables) >= 1:
        # 鍒涘缓瀛愭煡璇綔涓轰富琛?
        subquery_select = SelectNode()

        subquery_table = random.choice(tables)
        subquery_select.tables = [subquery_table]
        
        # 涓哄瓙鏌ヨ鐢熸垚鍞竴鐨勫唴閮ㄥ埆鍚?
        subquery_inner_alias = 's' + str(random.randint(100, 999))
        
        # 涓哄瓙鏌ヨ鐢熸垚FROM瀛愬彞
        subquery_from = FromNode()
        subquery_from.add_table(subquery_table, subquery_inner_alias)
        subquery_select.set_from_clause(subquery_from)
        
        # 涓哄瓙鏌ヨ鐢熸垚SELECT琛ㄨ揪寮忥紙鍙兘鍖呭惈鑱氬悎鍑芥暟锛?
        subquery_num_cols = random.randint(2, 4)  # 纭繚鑷冲皯鏈?涓猄ELECT琛ㄨ揪寮?
        subquery_non_aggregate = []
        subquery_has_aggregate = False
        
        for _ in range(subquery_num_cols):
            if random.random() > 0.4 and functions:
                # 娣诲姞鑱氬悎鍑芥暟
                agg_funcs = [f for f in functions if f.func_type == 'aggregate']
                if agg_funcs:
                    func = random.choice(agg_funcs)
                    func_node = FunctionCallNode(func)
                    param_count = func.min_params
                    if func.max_params is not None and func.max_params > func.min_params:
                        param_count = random.randint(func.min_params, func.max_params)
                    def build_literal_for_type(expected_type):
                        if expected_type == 'numeric':
                            return LiteralNode(random.randint(1, 100), 'INT')
                        if expected_type == 'string':
                            return LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                        if expected_type == 'datetime':
                            return LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                        if expected_type == 'json':
                            return LiteralNode('{"key": "value"}', 'JSON')
                        if expected_type == 'binary':
                            return create_geometry_literal_node(func.name)
                        if expected_type == 'boolean':
                            return LiteralNode(random.choice([True, False]), 'BOOLEAN')
                        return LiteralNode(random.randint(1, 100), 'INT')

                    for param_index in range(param_count):
                        col_ref = None
                        expected_types = func.param_types if hasattr(func, 'param_types') else ['any']
                        raw_expected_type = expected_types[param_index] if param_index < len(expected_types) else 'any'
                        expected_type = map_param_type_to_category(raw_expected_type)
                        expected_type = adjust_expected_type_for_min_max(func.name, param_index, expected_type)
                        expected_type = adjust_expected_type_for_conditionals(func.name, param_index, expected_type)
                        
                        matching_columns = []
                        for col in subquery_table.columns:
                            col_category = normalize_category(col.category, col.data_type)
                            if expected_type == 'any' or col_category == expected_type:
                                matching_columns.append(col)
                        
                        if matching_columns:
                            available_matching_columns = []
                            for col_candidate in matching_columns:
                                if not column_tracker.is_column_used(subquery_inner_alias, col_candidate.name):
                                    available_matching_columns.append(col_candidate)
                            
                            if available_matching_columns:
                                col = random.choice(available_matching_columns)
                                column_tracker.mark_column_as_used(subquery_inner_alias, col.name)
                            else:
                                col = random.choice(matching_columns)
                            
                            col_ref = ColumnReferenceNode(col, subquery_inner_alias)
                        else:
                            if expected_type == 'any':
                                col = get_random_column_with_tracker(subquery_table, subquery_inner_alias, column_tracker, for_select=True)
                                col_ref = ColumnReferenceNode(col, subquery_inner_alias)
                            else:
                                col_ref = build_literal_for_type(expected_type)
                        
                        added = func_node.add_child(col_ref)
                        if not added:
                            func_node.add_child(build_literal_for_type(expected_type))
                    
                    alias_suffix = random.randint(1, 1000)
                    subquery_select.add_select_expression(func_node, f'{func.name.lower()}_{alias_suffix}')
                    subquery_has_aggregate = True
            else:
                # 娣诲姞鏅€氬垪
                col = get_random_column_with_tracker(subquery_table, subquery_inner_alias, column_tracker, for_select=True)
                col_ref = ColumnReferenceNode(col, subquery_inner_alias)
                subquery_select.add_select_expression(col_ref, col.name)
                subquery_non_aggregate.append(col_ref)

        # 濡傛灉鏈夎仛鍚堝嚱鏁帮紝娣诲姞GROUP BY瀛愬彞
        if subquery_has_aggregate:
            if not subquery_non_aggregate:
                col = get_random_column_with_tracker(subquery_table, subquery_inner_alias, column_tracker, for_select=True)
                col_ref = ColumnReferenceNode(col, subquery_inner_alias)
                subquery_non_aggregate.append(col_ref)
            
            subquery_group_by = GroupByNode()
            for col_ref in subquery_non_aggregate:
                subquery_group_by.add_expression(col_ref)
            
            subquery_select.set_group_by_clause(subquery_group_by)

        # 涓哄瓙鏌ヨ鐢熸垚WHERE瀛愬彞锛堝彲閫夛級
        if random.random() > 0.5:
            col = subquery_table.get_random_column()
            col_ref = ColumnReferenceNode(col, subquery_inner_alias)
            safe_category = get_safe_comparison_category(col)
            operators = get_comparison_operators(safe_category)
            operators.extend(['IS NULL', 'IS NOT NULL'])
            operator = random.choice(operators)
            comp_node = ComparisonNode(operator)
            comp_node.add_child(col_ref)
            
            if operator not in ['IS NULL', 'IS NOT NULL']:
                if safe_category == 'numeric':
                    comp_node.add_child(LiteralNode(random.randint(0, 100), col.data_type))
                elif safe_category == 'string':
                    comp_node.add_child(LiteralNode(f"'sample_{random.randint(1, 100)}'", col.data_type))
                elif safe_category == 'datetime':
                    # 鐢熸垚鍖呭惈鏃跺垎绉掔殑闅忔満datetime
                    hours = random.randint(0, 23)
                    minutes = random.randint(0, 59)
                    seconds = random.randint(0, 59)
                    datetime_str = f"'2023-01-01 {hours:02d}:{minutes:02d}:{seconds:02d}'"
                    comp_node.add_child(LiteralNode(datetime_str, col.data_type))
                elif safe_category == 'binary':
                    # 涓篵inary绫诲瀷鐢熸垚鍚堥€傜殑浜岃繘鍒跺瓧闈㈤噺锛屼娇鐢ㄥ崄鍏繘鍒惰〃绀?
                    binary_len = random.randint(1, 10)  # 闅忔満鐢熸垚1-10瀛楄妭鐨勪簩杩涘埗鏁版嵁
                    hex_str = ''.join(random.choices('0123456789ABCDEF', k=binary_len*2))
                    comp_node.add_child(LiteralNode(f"X'{hex_str}'", col.data_type))
                elif safe_category == 'json':
                    comp_node.add_child(LiteralNode('{"key": "value"}', 'JSON'))
                elif safe_category == 'boolean':
                    comp_node.add_child(LiteralNode(random.choice([True, False]), 'BOOLEAN'))
            subquery_select.set_where_clause(comp_node)

        # 闅忔満娣诲姞ORDER BY瀛愬彞
        if random.random() > 0.4:
            order_by = OrderByNode()
            if subquery_has_aggregate and subquery_non_aggregate:
                # 濡傛灉鏈塆ROUP BY瀛愬彞锛屼粠GROUP BY鍒椾腑閫夋嫨
                col_ref = random.choice(subquery_non_aggregate)
            else:
                # 鍚﹀垯闅忔満閫夋嫨涓€鍒?
                col = subquery_table.get_random_column()
                col_ref = ColumnReferenceNode(col, subquery_inner_alias)
            order_by.add_expression(col_ref, random.choice(['ASC', 'DESC']))
            subquery_select.set_order_by_clause(order_by)

        # 闅忔満娣诲姞LIMIT瀛愬彞
        if random.random() > 0.5:
            subquery_select.set_limit_clause(LimitNode(random.randint(1, 10)))

        # 涓哄瓙鏌ヨ鐢熸垚鍒悕
        base_sub_alias = 'subq'
        main_alias = base_sub_alias if base_sub_alias not in sql_keywords else 'sub' + str(random.randint(0, 9))

        # 鍒涘缓瀛愭煡璇㈣妭鐐瑰苟娣诲姞鍒癋ROM瀛愬彞
        subquery_node = SubqueryNode(subquery_select, main_alias)
        from_node.add_table(subquery_node, main_alias)
        main_table = subquery_table  # 淇濆瓨涓昏〃淇℃伅鐢ㄤ簬鍚庣画澶勭悊
        # 瀛樺偍褰撳墠娣卞害淇℃伅
        subquery_node.metadata['depth'] = current_depth + 1
    else:
        main_table = random.choice(tables)
        base_alias = main_table.name[:3].lower()
        main_alias = base_alias if base_alias not in sql_keywords else main_table.name[:2].lower() + str(random.randint(0, 9))
        from_node.add_table(main_table, main_alias)

    # 闅忔満娣诲姞杩炴帴
    if random.random() > 0.3 and len(tables) > 1:
        join_table = random.choice([t for t in tables if t.name != main_table.name])
        # 鐢熸垚瀹夊叏鐨勮繛鎺ヨ〃鍒悕锛岄伩鍏峉QL鍏抽敭瀛楀啿绐?
        base_join_alias = join_table.name[:3].lower()
        join_alias = base_join_alias if base_join_alias not in sql_keywords else join_table.name[:2].lower() + str(random.randint(0, 9))

        # 鍒涘缓杩炴帴鏉′欢锛堝甫绫诲瀷妫€鏌ュ拰杞崲锛?
        fk = next((fk for fk in join_table.foreign_keys if fk["ref_table"] == main_table.name), None)
        if fk:
            # 鑾峰彇瀹為檯鐨勫垪瀵硅薄锛岃€屼笉鏄垱寤烘柊鐨?
            join_col = join_table.get_column(fk["column"])
            main_col = main_table.get_column(fk["ref_column"])
            
            if join_col and main_col:
                left_col_ref = ColumnReferenceNode(join_col, join_alias)
                right_col_ref = ColumnReferenceNode(main_col, main_alias)
                
                # 妫€鏌ュ垪绫诲瀷鏄惁鍖归厤
                if join_col.data_type.lower() != main_col.data_type.lower():
                    # 绫诲瀷涓嶅尮閰嶏紝杩涜鏄惧紡杞崲
                    # 宸插湪鏂囦欢椤堕儴瀵煎叆鎵€鏈夊繀瑕佺殑绫伙紝鏃犻渶灞€閮ㄥ鍏?
                    
                    # 纭畾杞崲鍑芥暟锛堣繖閲屼娇鐢–AST鍑芥暟浣滀负绀轰緥锛?
                    # 瀹為檯搴旂敤涓彲鑳介渶瑕佹牴鎹暟鎹簱鏂硅█鍜屽叿浣撶被鍨嬮€夋嫨鍚堥€傜殑杞崲鍑芥暟
                    if main_col.category == 'numeric' and join_col.category == 'string':
                        # 瀛楃涓茶浆鏁板€?
                        cast_func = Function('CAST', 2, 2, ['any', 'string'], 'numeric', 'scalar')
                        cast_node = FunctionCallNode(cast_func)
                        cast_node.add_child(left_col_ref)
                        cast_node.add_child(LiteralNode(main_col.data_type, 'NONE'))
                        left_col_ref = cast_node
                    elif main_col.category == 'string' and join_col.category == 'numeric':
                        # 鏁板€艰浆瀛楃涓?
                        cast_func = Function('CAST', 2, 2, ['any', 'string'], 'string', 'scalar')
                        cast_node = FunctionCallNode(cast_func)
                        cast_node.add_child(left_col_ref)
                        cast_node.add_child(LiteralNode(main_col.data_type, 'NONE'))
                        left_col_ref = cast_node
                    elif main_col.category == 'datetime' and join_col.category == 'string':
                        # 瀛楃涓茶浆鏃ユ湡鏃堕棿
                        cast_func = Function('CAST', 2, 2, ['any', 'string'], 'datetime', 'scalar')
                        cast_node = FunctionCallNode(cast_func)
                        cast_node.add_child(left_col_ref)
                        cast_node.add_child(LiteralNode(main_col.data_type, 'NONE'))
                        left_col_ref = cast_node
                    
                # 鍒涘缓杩炴帴鏉′欢
                condition = ComparisonNode("=")
                condition.add_child(left_col_ref)
                condition.add_child(right_col_ref)
            else:
                # 濡傛灉鏃犳硶鑾峰彇瀹為檯鍒楀璞★紝鍥為€€鍒板師鏉ョ殑瀹炵幇
                left_col = ColumnReferenceNode(
                    Column(fk["column"], "", "numeric", False, join_table.name),
                    join_alias
                )
                right_col = ColumnReferenceNode(
                    Column(fk["ref_column"], "", "numeric", False, main_table.name),
                    main_alias
                )
                condition = ComparisonNode("=")
                condition.add_child(left_col)
                condition.add_child(right_col)

            from_node.add_join(
                random.choice(["INNER", "LEFT", "RIGHT", "CROSS"]),
                join_table,
                join_alias,
                condition
            )

    select_node.set_from_clause(from_node)
        # 鍒濆鍖栧垪杩借釜鍣ㄧ殑鍙敤鍒椾俊鎭?
    if select_node.metadata and 'column_tracker' in select_node.metadata:
        column_tracker = select_node.metadata['column_tracker']
        if hasattr(column_tracker, 'initialize_from_from_node'):
            column_tracker.initialize_from_from_node(from_node)


    # 楠岃瘉鎵€鏈夊垪寮曠敤鐨勫埆鍚嶆槸鍚︽湁鏁?
    valid, errors = select_node.validate_all_columns()
    if not valid:
        # 濡傛灉楠岃瘉澶辫触锛屼慨澶嶆棤鏁堢殑鍒楀紩鐢?
        select_node.repair_invalid_columns()
        # 鍐嶆楠岃瘉
        valid, errors = select_node.validate_all_columns()
        
    # 娣诲姞SELECT琛ㄨ揪寮?

    num_columns = random.randint(1, 5)
    non_aggregate_columns = []  # 瀛樺偍闈炶仛鍚堝垪
    has_aggregate_function = False  # 璺熻釜鏄惁鍖呭惈鑱氬悎鍑芥暟
    used_aliases = set()  # 鐢ㄤ簬璺熻釜宸蹭娇鐢ㄧ殑鍒楀埆鍚?
    for _ in range(num_columns):
        if random.random() > 0.3 and functions:  # 30%姒傜巼浣跨敤鍑芥暟
            func = random.choice(functions)
            func_node = FunctionCallNode(func)

            # 涓哄嚱鏁版坊鍔犲弬鏁?
            # 纭繚鍙傛暟鏁伴噺鍦ㄦ湁鏁堣寖鍥村唴
            param_count = func.min_params
            if func.max_params is not None and func.max_params > func.min_params and not func.name.startswith('ST_'):
                param_count = random.randint(func.min_params, func.max_params)
            for param_idx in range(param_count):
                # 鏍规嵁鍑芥暟鍙傛暟绫诲瀷閫夋嫨鍚堥€傜殑鍒?
                expected_type = map_param_type_to_category(func.param_types[param_idx]) if param_idx < len(func.param_types) else 'any'
                expected_type = adjust_expected_type_for_min_max(func.name, param_idx, expected_type)
                expected_type = adjust_expected_type_for_conditionals(func.name, param_idx, expected_type)
                col_ref = None

                # 鏍规嵁涓昏〃绫诲瀷閫夋嫨鍒?
                if use_subquery:
                    # 浠庡瓙鏌ヨ鐨勫垪鍒悕涓€夋嫨
                    subquery_node = from_node.table_references[0]
                    if hasattr(subquery_node, 'column_alias_map'):
                        # 鑾峰彇瀛愭煡璇㈢殑鍒楀埆鍚?
                        valid_aliases = list(subquery_node.column_alias_map.keys())
                        tables_to_choose_with_aliases = []
                        for ref in from_node.table_references:
                            if isinstance(ref, Table):
                                tables_to_choose_with_aliases.append((ref, from_node.get_alias_for_table(ref)))
                        if not tables_to_choose_with_aliases and main_table and main_alias:
                            tables_to_choose_with_aliases = [(main_table, main_alias)]
                        # 鐗规畩澶勭悊DATE_FORMAT鍑芥暟鍜孋ONCAT鍑芥暟
                        # 缁熶竴澶勭悊涓変釜鍑芥暟鐨勭壒娈婇€昏緫
                        # 1. SUBSTRING鍑芥暟鐗规畩澶勭悊
                        if func.name == 'SUBSTRING':
                            # 绗竴涓弬鏁板繀椤绘槸瀛楃涓茬被鍨?
                            if param_idx == 0:
                                # 浼樺厛閫夋嫨瀛楃涓茬被鍨嬬殑鍒楀埆鍚?
                                string_aliases = []
                                for alias in valid_aliases:
                                    _, _, category = subquery_node.column_alias_map[alias]
                                    if category == 'string':
                                        string_aliases.append(alias)
                                
                                if string_aliases:
                                    alias = random.choice(string_aliases)
                                    col_name, data_type, category = subquery_node.column_alias_map[alias]
                                    col = Column(alias, data_type, category, False, main_alias)
                                    col_ref = ColumnReferenceNode(col, main_alias)
                                else:
                                    # 娌℃湁瀛楃涓茬被鍨嬪垪锛屼娇鐢ㄥ瓧闈㈤噺瀛楃涓?
                                    col_ref = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                            # 绗簩銆佷笁涓弬鏁板繀椤绘槸鏁板€肩被鍨嬶紙浣嶇疆鍜岄暱搴︼級
                            elif param_idx in [1, 2]:
                                # 鐢熸垚涓€涓悎鐞嗙殑鏁存暟鍊?
                                value = random.randint(1, 20) if param_idx == 1 else random.randint(1, 10)
                                col_ref = LiteralNode(value, 'INT')
                        
                        # 2. DATE_FORMAT/TO_CHAR鍑芥暟鐗规畩澶勭悊
                        elif (func.name == 'DATE_FORMAT' or func.name == 'TO_CHAR'):
                            # 绗竴涓弬鏁帮細鏃ユ湡鏃堕棿鍒?
                            if param_idx == 0:
                                # 淇锛氫娇鐢ㄦ纭殑琛ㄥ拰鍒悕閫夋嫨锛堢‘淇濊〃鍦‵ROM瀛愬彞涓級
                                if tables_to_choose_with_aliases:
                                    table, alias = random.choice(tables_to_choose_with_aliases)
                                    # 鏌ユ壘鏃ユ湡绫诲瀷鐨勫垪
                                    date_columns = [col for col in table.columns if col.category == 'datetime']
                                    if date_columns:
                                        # 鑾峰彇鍒楄窡韪櫒
                                        column_tracker = select_node.metadata.get('column_tracker')
                                        if column_tracker:
                                            # 杩囨护鍑烘湭浣跨敤鐨勬棩鏈熷垪
                                            available_date_columns = []
                                            for col in date_columns:
                                                col_identifier = f"{alias}.{col.name}"
                                                if not column_tracker.is_column_used(col_identifier):
                                                    available_date_columns.append(col)
                                            
                                            if available_date_columns:
                                                col = random.choice(available_date_columns)
                                                # 鏍囪鍒楀凡浣跨敤
                                                col_identifier = f"{alias}.{col.name}"
                                                column_tracker.mark_column_used(col_identifier)
                                            else:
                                                col = random.choice(date_columns)
                                        else:
                                            col = random.choice(date_columns)
                                        col_ref = ColumnReferenceNode(col, alias)
                                    else:
                                        # 淇锛氬綋娌℃湁鏃ユ湡绫诲瀷鍒楁椂锛屼娇鐢ㄦ棩鏈熷瓧闈㈤噺
                                        col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                                else:
                                    # 娌℃湁鍙敤琛紝浣跨敤鏃ユ湡瀛楅潰閲?
                                    col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                            # 绗簩涓弬鏁帮細鏍煎紡瀛楃涓插瓧闈㈤噺
                            elif param_idx == 1 and hasattr(func, 'format_string_required') and func.format_string_required:
                                # 绗簩涓弬鏁版槸鏍煎紡瀛楃涓?
                                # 鏍规嵁鍑芥暟绫诲瀷閫夋嫨姝ｇ‘鐨勬牸寮?
                                if func.name == 'TO_CHAR':
                                    # PostgreSQL TO_CHAR鏍煎紡
                                    format_strings = ['YYYY-MM-DD', 'YYYY-MM-DD HH24:MI:SS', 'DD-MON-YYYY', 'HH24:MI:SS']
                                else:
                                    # MySQL DATE_FORMAT鏍煎紡
                                    format_strings = ['%Y-%m-%d', '%Y-%m-%d %H:%i:%s', '%d-%b-%Y', '%H:%i:%s']
                                # 浣跨敤STRING绫诲瀷纭繚寮曞彿琚纭坊鍔?
                                col_ref = LiteralNode(random.choice(format_strings), 'STRING')
                        
                        # 3. CONCAT鍑芥暟鐗规畩澶勭悊锛氱‘淇濇墍鏈夊弬鏁伴兘鏄瓧绗︿覆绫诲瀷
                        elif func.name == 'CONCAT' and expected_type == 'string':
                            # 浼樺厛閫夋嫨瀛楃涓茬被鍨嬬殑鍒楀埆鍚?
                            string_aliases = []
                            for alias in valid_aliases:
                                _, _, category = subquery_node.column_alias_map[alias]
                                if category == 'string':
                                    string_aliases.append(alias)
                            
                            if string_aliases:
                                alias = random.choice(string_aliases)
                                col_name, data_type, category = subquery_node.column_alias_map[alias]
                                col = Column(alias, data_type, category, False, main_alias)
                                col_ref = ColumnReferenceNode(col, main_alias)
                            else:
                                # 娌℃湁瀛楃涓茬被鍨嬪垪锛屼娇鐢ㄥ瓧闈㈤噺瀛楃涓?
                                col_ref = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                        
                        # 4. NTILE鍑芥暟鐗规畩澶勭悊锛氱‘淇濆弬鏁版槸姝ｆ暣鏁帮紝涓嶅厑璁稿祵濂楀嚱鏁?
                        elif func.name == 'NTILE':
                            # 鐢熸垚涓€涓?-100涔嬮棿鐨勯殢鏈烘暣鏁颁綔涓哄弬鏁?
                            col_ref = LiteralNode(random.randint(1, 100), 'INT')
                        
                        # 5. NTH_VALUE鍑芥暟鐗规畩澶勭悊锛氱浜屼釜鍙傛暟蹇呴』鏄鏁存暟
                        elif func.name == 'NTH_VALUE' and param_idx == 1:
                            # 鐢熸垚涓€涓?-10涔嬮棿鐨勯殢鏈烘暣鏁颁綔涓哄弬鏁?
                            col_ref = LiteralNode(random.randint(1, 10), 'INT')
                        
                        # 6. CAST/CONVERT鍑芥暟鐗规畩澶勭悊锛氱‘淇濈浜屼釜鍙傛暟鏄湁鏁堢殑鏁版嵁绫诲瀷鍚嶇О
                        elif func.name in ['CAST', 'CONVERT']:
                            # 绗竴涓弬鏁板彲浠ユ槸浠绘剰琛ㄨ揪寮?
                            if param_idx == 0:
                                # 浼樺厛閫夋嫨鍒楀紩鐢?
                                if tables_to_choose_with_aliases:
                                    table, alias = random.choice(tables_to_choose_with_aliases)
                                    col = table.get_random_column()
                                    # 鑾峰彇鍒楄窡韪櫒
                                    column_tracker = select_node.metadata.get('column_tracker')
                                    
                                    if column_tracker:
                                        col_identifier = f"{alias}.{col.name}"
                                        if not column_tracker.is_column_used(col_identifier):
                                            column_tracker.mark_column_used(col_identifier)
                                    
                                    col_ref = ColumnReferenceNode(col, alias)
                                else:
                                    # 娌℃湁鍙敤琛紝浣跨敤瀛楅潰閲?
                                    col_ref = LiteralNode(random.randint(1, 100), 'INT')
                            # 绗簩涓弬鏁板繀椤绘槸鏈夋晥鐨勬暟鎹被鍨嬪悕绉?
                            elif param_idx == 1:
                                # 瀹氫箟涓€浜涘父瑙佺殑鏁版嵁绫诲瀷
                                data_types = get_cast_types()
                                # 闅忔満閫夋嫨涓€涓暟鎹被鍨?
                                data_type = random.choice(data_types)
                                if data_type == 'CHAR':
                                    # VARCHAR绫诲瀷闇€瑕佹寚瀹氶暱搴?
                                    length = random.randint(1, 255)
                                    data_type = f"{data_type}({length})"
                                
                                # 鍒涘缓涓€涓猄TRING绫诲瀷鐨凩iteralNode
                                col_ref = LiteralNode(data_type, "STRING")
                        
                        # 7. DATETIME鍑芥暟鐗规畩澶勭悊锛氬弬鏁板繀椤绘槸1-6鐨勬暣鏁?
                        elif func.name == 'DATETIME':
                            # 鐢熸垚1-6涔嬮棿鐨勯殢鏈烘暣鏁颁綔涓哄弬鏁?
                            col_ref = LiteralNode(random.randint(1, 6), 'INT')
                        
                        # 8. LEAD/LAG鍑芥暟鐗规畩澶勭悊锛氱浜屼釜鍙傛暟蹇呴』鏄暣鏁板父閲?
                        elif func.name in ['LEAD', 'LAG']:
                            if param_idx == 1:  # 绗簩涓弬鏁帮細鍋忕Щ閲?
                                # 鐢熸垚1-10涔嬮棿鐨勯殢鏈烘暣鏁颁綔涓哄亸绉婚噺
                                col_ref = LiteralNode(random.randint(1, 10), 'INT')
                            elif param_idx == 2:  # 绗笁涓弬鏁帮細榛樿鍊?
                                # 榛樿鍊煎彲浠ユ槸NULL鎴栦笌绗竴涓弬鏁扮被鍨嬪吋瀹圭殑鍊?
                                if random.random() < 0.5:
                                    col_ref = LiteralNode('NULL', 'NULL')
                                else:
                                    # 閫夋嫨涓庣涓€涓弬鏁扮被鍨嬪吋瀹圭殑鍊?
                                    col_ref = LiteralNode(random.randint(1, 100), 'INT')
                            else:  # 绗竴涓弬鏁帮細鍒楀紩鐢?
                                # 浼樺厛閫夋嫨鍒楀紩鐢?
                                if tables_to_choose_with_aliases:
                                    table, alias = random.choice(tables_to_choose_with_aliases)
                                    col = table.get_random_column()
                                    # 鑾峰彇鍒楄窡韪櫒
                                    column_tracker = select_node.metadata.get('column_tracker')
                                    
                                    if column_tracker:
                                        col_identifier = f"{alias}.{col.name}"
                                        if not column_tracker.is_column_used(col_identifier):
                                            column_tracker.mark_column_used(col_identifier)
                                    
                                    col_ref = ColumnReferenceNode(col, alias)
                                else:
                                    # 娌℃湁鍙敤琛紝浣跨敤瀛楅潰閲?
                                    col_ref = LiteralNode(random.randint(1, 100), 'INT')
                        
                        elif func.name in ['TIMESTAMPDIFF'] and param_idx == 0:
                            
                            data_type = random.choice(['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND'])
                            col_ref = LiteralNode(data_type, 'STRING')
                        elif func.name == 'ST_Transform' and param_idx == 1:
                            col_ref = LiteralNode(4326, 'INT')
                        elif func.name in ['JSON_VALUE', 'JSON_REMOVE', 'JSON_EXTRACT', 'JSON_SET','JSON_INSERT', 'JSON_REPLACE'] and param_idx == 1:
                            # 绗竴涓弬鏁帮細JSON閿?
                            literal = LiteralNode('$.key', 'STRING')
                            func_node.add_child(literal)
                        elif expected_type == 'string' and is_geohash_function(func.name):
                            col_ref = create_geohash_literal_node()
                        elif expected_type == 'string' and func.name.startswith('ST_') and is_wkt_function(func.name):
                            col_ref = create_wkt_literal_node(func.name)
                        elif expected_type == 'json':
                            json_aliases = [alias for alias in valid_aliases if subquery_node.column_alias_map[alias][2] == 'json']
                            if json_aliases:
                                alias = random.choice(json_aliases)
                                col_name, data_type, category = subquery_node.column_alias_map[alias]
                                col = Column(alias, data_type, category, False, main_alias)
                                col_ref = ColumnReferenceNode(col, main_alias)
                            else:
                                col_ref = LiteralNode('{"type":"Point","coordinates":[0,0]}', 'JSON')
                        elif expected_type == 'binary' and func.name.startswith('ST_'):
                            if 'FromWKB' in func.name:
                                col_ref = create_wkb_literal_node(func.name)
                            else:
                                required_geom = get_required_geometry_type(func.name)
                                geom_aliases = []
                                for alias in valid_aliases:
                                    col_name, data_type, category = subquery_node.column_alias_map[alias]
                                    if is_geometry_type(data_type) and _matches_required_geometry(
                                        Column(alias, data_type, category, False, main_alias),
                                        required_geom,
                                    ):
                                        geom_aliases.append(alias)
                                if geom_aliases:
                                    alias = random.choice(geom_aliases)
                                    col_name, data_type, category = subquery_node.column_alias_map[alias]
                                    col = Column(alias, data_type, category, False, main_alias)
                                    col_ref = ColumnReferenceNode(col, main_alias)
                                else:
                                    col_ref = create_geometry_literal_node(func.name)
                        elif expected_type == 'boolean':
                            boolean_aliases = [alias for alias in valid_aliases if subquery_node.column_alias_map[alias][2] == 'boolean']
                            if boolean_aliases:
                                alias = random.choice(boolean_aliases)
                                col_name, data_type, category = subquery_node.column_alias_map[alias]
                                col = Column(alias, data_type, category, False, main_alias)
                                col_ref = ColumnReferenceNode(col, main_alias)
                            else:
                                col_ref = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                        elif valid_aliases:
                            # 灏濊瘯鎵惧埌鍖归厤绫诲瀷鐨勫垪
                            matching_aliases = []
                            for alias in valid_aliases:
                                _, data_type, category = subquery_node.column_alias_map[alias]
                                if expected_type == 'any' or category == expected_type or (expected_type == 'numeric' and category in ['int', 'float', 'decimal']):
                                    matching_aliases.append(alias)

                            if matching_aliases:
                                alias = random.choice(matching_aliases)
                                col_name, data_type, category = subquery_node.column_alias_map[alias]
                                # 鍒涘缓寮曠敤瀛愭煡璇㈠垪鍒悕鐨勫垪寮曠敤
                                col = Column(alias, data_type, category, False, main_alias)
                                col_ref = ColumnReferenceNode(col, main_alias)
                            else:
                                # 娌℃湁鍖归厤绫诲瀷鐨勫垪锛屼娇鐢ㄥ洖閫€鏂规
                                if expected_type == 'numeric':
                                    col_ref = LiteralNode(random.randint(1, 100), 'INT')
                                elif expected_type == 'string':
                                    col_ref = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                                elif expected_type == 'datetime':
                                    col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                                elif expected_type == 'json':
                                    col_ref = LiteralNode('{"type":"Point","coordinates":[0,0]}', 'JSON')
                                elif expected_type == 'binary':
                                    col_ref = create_wkb_literal_node(func.name) if func.name.startswith('ST_') and 'FromWKB' in func.name else create_geometry_literal_node(func.name)
                                elif expected_type == 'boolean':
                                    col_ref = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                                else:
                                    alias = random.choice(valid_aliases)
                                    col_name, data_type, category = subquery_node.column_alias_map[alias]
                                    col = Column(alias, data_type, category, False, main_alias)
                                    col_ref = ColumnReferenceNode(col, main_alias)
                    else:
                        # 鍥為€€鏂规
                        col = main_table.get_random_column()
                        col_ref = ColumnReferenceNode(col, main_alias)
                else:
                        # 浠庢櫘閫氳〃涓€夋嫨鍒?
                        # 淇锛氬彧浣跨敤FROM瀛愬彞涓疄闄呭寘鍚殑琛?
                        available_tables = []
                        for ref in from_node.table_references:
                            if isinstance(ref, Table):
                                available_tables.append((ref, from_node.get_alias_for_table(ref)))
                            elif isinstance(ref, SubqueryNode):
                                available_tables.append((ref, ref.alias))
                        
                        tables_to_choose_with_aliases = available_tables if available_tables else [(main_table, main_alias)]
                        # 淇濈暀琛ㄥ拰鍒悕鐨勫搴斿叧绯伙紝涓嶅崟鐙垱寤簍ables_to_choose鍙橀噺
                        
                        # 鐗规畩澶勭悊DATE_FORMAT鍑芥暟鍜孋ONCAT鍑芥暟
                        # 缁熶竴澶勭悊涓変釜鍑芥暟鐨勭壒娈婇€昏緫
                        # 1. SUBSTRING鍑芥暟鐗规畩澶勭悊
                        if func.name == 'SUBSTRING':
                            # 绗竴涓弬鏁板繀椤绘槸瀛楃涓茬被鍨?
                            if param_idx == 0:
                                # 浼樺厛閫夋嫨瀛楃涓茬被鍨嬬殑鍒?
                                string_columns = []
                                for table, alias in tables_to_choose_with_aliases:
                                    for col in table.columns:
                                        if col.category == 'string':
                                            string_columns.append((table, col, alias))
                                
                                # 鑾峰彇鍒楄窡韪櫒
                                column_tracker = select_node.metadata.get('column_tracker')
                                
                                if string_columns:
                                    # 浣跨敤鍒楄窡韪櫒閫夋嫨鏈娇鐢ㄧ殑鍒?
                                    if column_tracker:
                                        # 杩囨护鍑烘湭浣跨敤鐨勫垪
                                        available_string_columns = []
                                        for table, col, alias in string_columns:
                                            col_identifier = f"{alias}.{col.name}"
                                            if not column_tracker.is_column_used(col_identifier):
                                                available_string_columns.append((table, col, alias))
                                        
                                        if available_string_columns:
                                            table, col, alias = random.choice(available_string_columns)
                                            # 鏍囪鍒楀凡浣跨敤
                                            col_identifier = f"{alias}.{col.name}"
                                            column_tracker.mark_column_used(col_identifier)
                                            col_ref = ColumnReferenceNode(col, alias)
                                        else:
                                            # 濡傛灉娌℃湁鏈娇鐢ㄧ殑鍒楋紝鍥為€€鍒伴殢鏈洪€夋嫨
                                            table, col, alias = random.choice(string_columns)
                                            col_ref = ColumnReferenceNode(col, alias)
                                    else:
                                        table, col, alias = random.choice(string_columns)
                                        col_ref = ColumnReferenceNode(col, alias)
                                else:
                                    # 娌℃湁瀛楃涓茬被鍨嬪垪锛屼娇鐢ㄥ瓧闈㈤噺瀛楃涓?
                                    col_ref = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                            # 绗簩銆佷笁涓弬鏁板繀椤绘槸鏁板€肩被鍨嬶紙浣嶇疆鍜岄暱搴︼級
                            elif param_idx in [1, 2]:
                                # 鐢熸垚涓€涓悎鐞嗙殑鏁存暟鍊?
                                value = random.randint(1, 20) if param_idx == 1 else random.randint(1, 10)
                                col_ref = LiteralNode(value, 'INT')
                        
                        # 2. DATE_FORMAT/TO_CHAR鍑芥暟鐗规畩澶勭悊
                        elif (func.name == 'DATE_FORMAT' or func.name == 'TO_CHAR'):
                            # 绗竴涓弬鏁帮細鏃ユ湡鏃堕棿鍒?
                            if param_idx == 0:
                                # 浼樺厛閫夋嫨鏃ユ湡鏃堕棿绫诲瀷鐨勫垪
                                datetime_columns = []
                                for table, alias in tables_to_choose_with_aliases:
                                    for col in table.columns:
                                        if col.category == 'datetime':
                                            datetime_columns.append((table, col, alias))
                                
                                # 鑾峰彇鍒楄窡韪櫒
                                column_tracker = select_node.metadata.get('column_tracker')
                                
                                if datetime_columns:
                                    # 浣跨敤鍒楄窡韪櫒閫夋嫨鏈娇鐢ㄧ殑鍒?
                                    if column_tracker:
                                        # 杩囨护鍑烘湭浣跨敤鐨勫垪
                                        available_datetime_columns = []
                                        for table, col, alias in datetime_columns:
                                            col_identifier = f"{alias}.{col.name}"
                                            if not column_tracker.is_column_used(col_identifier):
                                                available_datetime_columns.append((table, col, alias))
                                        
                                        if available_datetime_columns:
                                            table, col, alias = random.choice(available_datetime_columns)
                                            # 鏍囪鍒楀凡浣跨敤
                                            col_identifier = f"{alias}.{col.name}"
                                            column_tracker.mark_column_used(col_identifier)
                                            col_ref = ColumnReferenceNode(col, alias)
                                        else:
                                            # 濡傛灉娌℃湁鏈娇鐢ㄧ殑鍒楋紝鍥為€€鍒伴殢鏈洪€夋嫨
                                            table, col, alias = random.choice(datetime_columns)
                                            col_ref = ColumnReferenceNode(col, alias)
                                    else:
                                        table, col, alias = random.choice(datetime_columns)
                                        col_ref = ColumnReferenceNode(col, alias)
                                else:
                                    # 娌℃湁鏃ユ湡鏃堕棿绫诲瀷鍒楋紝浣跨敤鏃ユ湡瀛楅潰閲?
                                    col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                            # 绗簩涓弬鏁帮細鏍煎紡瀛楃涓插瓧闈㈤噺
                            elif param_idx == 1 and hasattr(func, 'format_string_required') and func.format_string_required:
                                # 涓篋ATE_FORMAT鍜孴O_CHAR鍑芥暟鎻愪緵鏈夋晥鐨勬棩鏈熸牸寮忓瓧绗︿覆
                                # MySQL鐨凞ATE_FORMAT浣跨敤鐧惧垎鍙锋牸寮?
                                # PostgreSQL鐨凾O_CHAR浣跨敤涓嶅甫鐧惧垎鍙风殑鏍煎紡
                                if func.name == 'TO_CHAR':
                                    # PostgreSQL TO_CHAR鏍煎紡
                                    format_strings = ['YYYY-MM-DD', 'YYYY-MM-DD HH24:MI:SS', 'DD-MON-YYYY', 'HH24:MI:SS']
                                else:
                                    # MySQL DATE_FORMAT鏍煎紡
                                    format_strings = ['%Y-%m-%d', '%Y-%m-%d %H:%i:%s', '%d-%b-%Y', '%H:%i:%s']
                                # 浣跨敤STRING绫诲瀷纭繚寮曞彿琚纭坊鍔狅紝涓嶈鍦ㄥ€间腑鐩存帴娣诲姞鍗曞紩鍙?
                                col_ref = LiteralNode(random.choice(format_strings), 'STRING')
                        
                        # 3. CONCAT鍑芥暟鐗规畩澶勭悊锛氱‘淇濇墍鏈夊弬鏁伴兘鏄瓧绗︿覆绫诲瀷
                        elif func.name == 'CONCAT' and expected_type == 'string':
                            # 浼樺厛閫夋嫨瀛楃涓茬被鍨嬬殑鍒?
                            string_columns = []
                            for table, alias in tables_to_choose_with_aliases:
                                for col in table.columns:
                                    if col.category == 'string':
                                        string_columns.append((table, col, alias))
                            
                            # 鑾峰彇鍒楄窡韪櫒
                            column_tracker = select_node.metadata.get('column_tracker')
                            
                            if string_columns:
                                # 浣跨敤鍒楄窡韪櫒閫夋嫨鏈娇鐢ㄧ殑鍒?
                                if column_tracker:
                                    # 杩囨护鍑烘湭浣跨敤鐨勫垪
                                    available_string_columns = []
                                    for table, col, alias in string_columns:
                                        col_identifier = f"{alias}.{col.name}"
                                        if not column_tracker.is_column_used(col_identifier):
                                            available_string_columns.append((table, col, alias))
                                    
                                    if available_string_columns:
                                        table, col, alias = random.choice(available_string_columns)
                                        # 鏍囪鍒楀凡浣跨敤
                                        column_tracker.mark_column_as_used(alias, col.name)
                                        col_ref = ColumnReferenceNode(col, alias)
                                    else:
                                        # 濡傛灉娌℃湁鏈娇鐢ㄧ殑鍒楋紝鍥為€€鍒伴殢鏈洪€夋嫨
                                        table, col, alias = random.choice(string_columns)
                                        col_ref = ColumnReferenceNode(col, alias)
                                else:
                                    table, col, alias = random.choice(string_columns)
                                    col_ref = ColumnReferenceNode(col, alias)
                            else:
                                # 娌℃湁瀛楃涓茬被鍨嬪垪锛屼娇鐢ㄥ瓧闈㈤噺瀛楃涓?
                                col_ref = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                        
                        # 4. NTILE鍑芥暟鐗规畩澶勭悊锛氱‘淇濆弬鏁版槸姝ｆ暣鏁帮紝涓嶅厑璁稿祵濂楀嚱鏁?
                        elif func.name == 'NTILE':
                            # 鐢熸垚涓€涓?-100涔嬮棿鐨勯殢鏈烘暣鏁颁綔涓哄弬鏁?
                            col_ref = LiteralNode(random.randint(1, 100), 'INT')
                        
                        # 5. NTH_VALUE鍑芥暟鐗规畩澶勭悊锛氱浜屼釜鍙傛暟蹇呴』鏄鏁存暟
                        elif func.name == 'NTH_VALUE' and param_idx == 1:
                            # 鐢熸垚涓€涓?-10涔嬮棿鐨勯殢鏈烘暣鏁颁綔涓哄弬鏁?
                            col_ref = LiteralNode(random.randint(1, 10), 'INT')
                        
                        # 6. CAST/CONVERT鍑芥暟鐗规畩澶勭悊锛氱‘淇濈浜屼釜鍙傛暟鏄湁鏁堢殑鏁版嵁绫诲瀷鍚嶇О
                        elif func.name in ['CAST', 'CONVERT']:
                            # 绗竴涓弬鏁板彲浠ユ槸浠绘剰琛ㄨ揪寮?
                            if param_idx == 0:
                                # 浼樺厛閫夋嫨鍒楀紩鐢?
                                if tables_to_choose_with_aliases:
                                    table, alias = random.choice(tables_to_choose_with_aliases)
                                    col = table.get_random_column()
                                    # 鑾峰彇鍒楄窡韪櫒
                                    column_tracker = select_node.metadata.get('column_tracker')
                                    
                                    if column_tracker:
                                        col_identifier = f"{alias}.{col.name}"
                                        if not column_tracker.is_column_used(col_identifier):
                                            column_tracker.mark_column_used(col_identifier)
                                    
                                    col_ref = ColumnReferenceNode(col, alias)
                                else:
                                    # 娌℃湁鍙敤琛紝浣跨敤瀛楅潰閲?
                                    col_ref = LiteralNode(random.randint(1, 100), 'INT')
                            # 绗簩涓弬鏁板繀椤绘槸鏈夋晥鐨勬暟鎹被鍨嬪悕绉?
                            elif param_idx == 1:
                                # 瀹氫箟涓€浜涘父瑙佺殑鏁版嵁绫诲瀷
                                data_types = get_cast_types()
                                # 闅忔満閫夋嫨涓€涓暟鎹被鍨?
                                data_type = random.choice(data_types)
                                if data_type == 'CHAR':
                                    # VARCHAR绫诲瀷闇€瑕佹寚瀹氶暱搴?
                                    length = random.randint(1, 255)
                                    data_type = f"{data_type}({length})"
                                
                                # 鍒涘缓涓€涓猄TRING绫诲瀷鐨凩iteralNode
                                col_ref = LiteralNode(data_type, "STRING")
                        
                        # 7. DATETIME鍑芥暟鐗规畩澶勭悊锛氬弬鏁板繀椤绘槸1-6鐨勬暣鏁?
                        elif func.name == 'DATETIME':
                            # 鐢熸垚1-6涔嬮棿鐨勯殢鏈烘暣鏁颁綔涓哄弬鏁?
                            col_ref = LiteralNode(random.randint(1, 6), 'INT')
                        
                        # 8. LEAD/LAG鍑芥暟鐗规畩澶勭悊锛氱浜屼釜鍙傛暟蹇呴』鏄暣鏁板父閲?
                        elif func.name in ['LEAD', 'LAG']:
                            if param_idx == 1:  # 绗簩涓弬鏁帮細鍋忕Щ閲?
                                # 鐢熸垚1-10涔嬮棿鐨勯殢鏈烘暣鏁颁綔涓哄亸绉婚噺
                                col_ref = LiteralNode(random.randint(1, 10), 'INT')
                            elif param_idx == 2:  # 绗笁涓弬鏁帮細榛樿鍊?
                                # 榛樿鍊煎彲浠ユ槸NULL鎴栦笌绗竴涓弬鏁扮被鍨嬪吋瀹圭殑鍊?
                                if random.random() < 0.5:
                                    col_ref = LiteralNode('NULL', 'NULL')
                                else:
                                    # 閫夋嫨涓庣涓€涓弬鏁扮被鍨嬪吋瀹圭殑鍊?
                                    col_ref = LiteralNode(random.randint(1, 100), 'INT')
                            else:  # 绗竴涓弬鏁帮細鍒楀紩鐢?
                                # 浼樺厛閫夋嫨鍒楀紩鐢?
                                if tables_to_choose_with_aliases:
                                    table, alias = random.choice(tables_to_choose_with_aliases)
                                    col = table.get_random_column()
                                    # 鑾峰彇鍒楄窡韪櫒
                                    column_tracker = select_node.metadata.get('column_tracker')
                                    
                                    if column_tracker:
                                        col_identifier = f"{alias}.{col.name}"
                                        if not column_tracker.is_column_used(col_identifier):
                                            column_tracker.mark_column_used(col_identifier)
                                    
                                    col_ref = ColumnReferenceNode(col, alias)
                                else:
                                    # 娌℃湁鍙敤琛紝浣跨敤瀛楅潰閲?
                                    col_ref = LiteralNode(random.randint(1, 100), 'INT')
                        
                        
                        elif func.name in ['TIMESTAMPDIFF'] and param_idx == 0:

                            data_type = random.choice(['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND'])
                            col_ref = LiteralNode(data_type, 'STRING')
                        elif func.name == 'ST_Transform' and param_idx == 1:
                            col_ref = LiteralNode(4326, 'INT')
                        elif func.name in ['JSON_VALUE', 'JSON_REMOVE', 'JSON_EXTRACT', 'JSON_SET','JSON_INSERT', 'JSON_REPLACE'] and param_idx == 1:
                            # 绗竴涓弬鏁帮細JSON閿?
                            literal = LiteralNode('$.key', 'STRING')
                            func_node.add_child(literal)
                        elif expected_type == 'string' and is_geohash_function(func.name):
                            col_ref = create_geohash_literal_node()
                        elif expected_type == 'string' and func.name.startswith('ST_') and is_wkt_function(func.name):
                            col_ref = create_wkt_literal_node(func.name)
                        elif expected_type == 'json':
                            json_columns = []
                            for table, alias in tables_to_choose_with_aliases:
                                for col in table.columns:
                                    if col.category == 'json':
                                        json_columns.append((table, col, alias))
                            if json_columns:
                                table, col, alias = random.choice(json_columns)
                                col_ref = ColumnReferenceNode(col, alias)
                            else:
                                col_ref = LiteralNode('{"type":"Point","coordinates":[0,0]}', 'JSON')
                        elif expected_type == 'binary' and func.name.startswith('ST_'):
                            if 'FromWKB' in func.name:
                                col_ref = create_wkb_literal_node(func.name)
                            else:
                                required_geom = get_required_geometry_type(func.name)
                                geometry_columns = []
                                for table, alias in tables_to_choose_with_aliases:
                                    for col in table.columns:
                                        if is_geometry_type(col.data_type) and _matches_required_geometry(col, required_geom):
                                            geometry_columns.append((table, col, alias))
                                if geometry_columns:
                                    table, col, alias = random.choice(geometry_columns)
                                    col_ref = ColumnReferenceNode(col, alias)
                                else:
                                    col_ref = create_geometry_literal_node(func.name)
                        elif expected_type == 'boolean':
                            boolean_columns = []
                            for table, alias in tables_to_choose_with_aliases:
                                for col in table.columns:
                                    if col.category == 'boolean':
                                        boolean_columns.append((table, col, alias))
                            if boolean_columns:
                                table, col, alias = random.choice(boolean_columns)
                                col_ref = ColumnReferenceNode(col, alias)
                            else:
                                col_ref = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                        else:
                            # 灏濊瘯鎵惧埌鍖归厤绫诲瀷鐨勫垪
                            matching_columns = []
                            for table, alias in tables_to_choose_with_aliases:
                                for col in table.columns:
                                    if expected_type == 'any' or col.category == expected_type or (expected_type == 'numeric' and col.category in ['int', 'float', 'decimal']):
                                        matching_columns.append((table, col, alias))

                            # 鑾峰彇鍒楄窡韪櫒
                            column_tracker = select_node.metadata.get('column_tracker')

                            if matching_columns:
                                # 浣跨敤鍒楄窡韪櫒閫夋嫨鏈娇鐢ㄧ殑鍒?
                                if column_tracker:
                                    # 杩囨护鍑烘湭浣跨敤鐨勫垪
                                    available_matching_columns = []
                                    for table, col, alias in matching_columns:
                                        col_identifier = f"{alias}.{col.name}"
                                        if not column_tracker.is_column_used(col_identifier):
                                            available_matching_columns.append((table, col, alias))
                                    
                                    if available_matching_columns:
                                        table, col, alias = random.choice(available_matching_columns)
                                        # 鏍囪鍒楀凡浣跨敤
                                        column_tracker.mark_column_as_used(alias, col.name)
                                        col_ref = ColumnReferenceNode(col, alias)
                                    else:
                                        # 濡傛灉娌℃湁鏈娇鐢ㄧ殑鍒楋紝鍥為€€鍒伴殢鏈洪€夋嫨
                                        table, col, alias = random.choice(matching_columns)
                                        col_ref = ColumnReferenceNode(col, alias)
                                else:
                                    table, col, alias = random.choice(matching_columns)
                                    col_ref = ColumnReferenceNode(col, alias)
                            else:
                                # 娌℃湁鍖归厤绫诲瀷鐨勫垪锛屼娇鐢ㄥ洖閫€鏂规
                                if expected_type == 'numeric':
                                    col_ref = LiteralNode(random.randint(1, 100), 'INT')
                                elif expected_type == 'string':
                                    col_ref = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                                elif expected_type == 'datetime':
                                    col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                                elif expected_type == 'json':
                                    col_ref = LiteralNode('{"type":"Point","coordinates":[0,0]}', 'JSON')
                                elif expected_type == 'binary':
                                    col_ref = create_wkb_literal_node(func.name) if func.name.startswith('ST_') and 'FromWKB' in func.name else create_geometry_literal_node(func.name)
                                elif expected_type == 'boolean':
                                    col_ref = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                                else:
                                    table, alias = random.choice(tables_to_choose_with_aliases)
                                    col = table.get_random_column()
                                    col_ref = ColumnReferenceNode(col, alias)
                # 纭繚鑱氬悎鍑芥暟鎬绘槸鏈夊弬鏁?
                if func.func_type == 'aggregate':
                    # 澧炲己鐗堝弬鏁颁繚闅滄満鍒?
                    if not col_ref:
                        if use_subquery:
                            subquery_node = from_node.table_references[0]
                            if hasattr(subquery_node, 'column_alias_map') and subquery_node.column_alias_map:
                                valid_aliases = list(subquery_node.column_alias_map.keys())
                                candidate_aliases = valid_aliases
                                if expected_type != 'any':
                                    candidate_aliases = []
                                    for alias in valid_aliases:
                                        _, data_type, category = subquery_node.column_alias_map[alias]
                                        if normalize_category(category, data_type) == expected_type:
                                            candidate_aliases.append(alias)
                                if candidate_aliases:
                                    alias = random.choice(candidate_aliases)
                                    col_name, data_type, category = subquery_node.column_alias_map[alias]
                                    col = Column(alias, data_type, category, False, main_alias)
                                    col_ref = ColumnReferenceNode(col, main_alias)
                                else:
                                    if expected_type == 'numeric':
                                        col_ref = LiteralNode(random.randint(1, 100), 'INT')
                                    elif expected_type == 'string':
                                        col_ref = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                                    elif expected_type == 'datetime':
                                        col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                                    elif expected_type == 'json':
                                        col_ref = LiteralNode('{"type":"Point","coordinates":[0,0]}', 'JSON')
                                    elif expected_type == 'binary':
                                        col_ref = create_wkb_literal_node(func.name) if func.name.startswith('ST_') and 'FromWKB' in func.name else create_geometry_literal_node(func.name)
                                    elif expected_type == 'boolean':
                                        col_ref = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                                    else:
                                        col_ref = LiteralNode(random.randint(1, 100), 'INT')
                            else:
                                # 瀛愭煡璇㈡病鏈夊垪鍒悕鏄犲皠锛岀洿鎺ヤ娇鐢ㄤ富琛ㄥ垪
                                if expected_type != 'any':
                                    matching = [col for col in main_table.columns if normalize_category(col.category, col.data_type) == expected_type]
                                    if matching:
                                        col = random.choice(matching)
                                        col_ref = ColumnReferenceNode(col, main_alias)
                                    elif expected_type == 'numeric':
                                        col_ref = LiteralNode(random.randint(1, 100), 'INT')
                                    elif expected_type == 'string':
                                        col_ref = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                                    elif expected_type == 'datetime':
                                        col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                                    elif expected_type == 'json':
                                        col_ref = LiteralNode('{"type":"Point","coordinates":[0,0]}', 'JSON')
                                    elif expected_type == 'binary':
                                        col_ref = create_wkb_literal_node(func.name) if func.name.startswith('ST_') and 'FromWKB' in func.name else create_geometry_literal_node(func.name)
                                    elif expected_type == 'boolean':
                                        col_ref = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                                else:
                                    col = main_table.get_random_column()
                                    col_ref = ColumnReferenceNode(col, main_alias)
                        else:
                            # 浠庡彲鐢ㄨ〃涓€夋嫨鍒?
                            if tables_to_choose:
                                # 纭繚閫夋嫨鐨勮〃鍜屽埆鍚嶅湪FROM瀛愬彞涓湁鏁?
                                valid_table_found = False
                                while not valid_table_found and tables_to_choose:
                                    table = random.choice(tables_to_choose)
                                    alias = main_alias if table == main_table else join_alias
                                    # 楠岃瘉鍒悕鏄惁鍦‵ROM瀛愬彞涓畾涔?
                                    if from_node.get_table_for_alias(alias):
                                        if expected_type != 'any':
                                            matching = [c for c in table.columns if normalize_category(c.category, c.data_type) == expected_type]
                                            if matching:
                                                col = random.choice(matching)
                                                col_ref = ColumnReferenceNode(col, alias)
                                            elif expected_type == 'numeric':
                                                col_ref = LiteralNode(random.randint(1, 100), 'INT')
                                            elif expected_type == 'string':
                                                col_ref = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                                            elif expected_type == 'datetime':
                                                col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                                            elif expected_type == 'json':
                                                col_ref = LiteralNode('{"type":"Point","coordinates":[0,0]}', 'JSON')
                                            elif expected_type == 'binary':
                                                col_ref = create_wkb_literal_node(func.name) if func.name.startswith('ST_') and 'FromWKB' in func.name else create_geometry_literal_node(func.name)
                                            elif expected_type == 'boolean':
                                                col_ref = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                                        else:
                                            col = table.get_random_column()
                                            col_ref = ColumnReferenceNode(col, alias)
                                        valid_table_found = True
                                    else:
                                        # 濡傛灉鍒悕鏃犳晥锛屼粠閫夋嫨鍒楄〃涓Щ闄よ琛?
                                        tables_to_choose.remove(table)
                                # 濡傛灉鎵€鏈夎〃閮芥棤鏁堬紝浣跨敤涓昏〃
                                if not valid_table_found and main_table:
                                    alias = main_alias
                                    col = main_table.get_random_column()
                                    col_ref = ColumnReferenceNode(col, alias)
                            else:
                                # 娌℃湁鍙敤琛紝鍒涘缓瀛楅潰閲忓弬鏁?
                                if expected_type == 'numeric':
                                    col_ref = LiteralNode(random.randint(1, 100), 'INT')
                                elif expected_type == 'string':
                                    col_ref = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                                elif expected_type == 'datetime':
                                    col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                                elif expected_type == 'json':
                                    col_ref = LiteralNode('{"type":"Point","coordinates":[0,0]}', 'JSON')
                                elif expected_type == 'binary':
                                    col_ref = create_wkb_literal_node(func.name) if func.name.startswith('ST_') and 'FromWKB' in func.name else create_geometry_literal_node(func.name)
                                elif expected_type == 'boolean':
                                    col_ref = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                                else:
                                    # 榛樿涓烘暟鍊煎瀷
                                    col_ref = LiteralNode(random.randint(1, 100), 'INT')
                    # 缁堟瀬淇濋殰锛氬鏋滄墍鏈夊垪寮曠敤鏂规閮藉け璐ワ紝鐩存帴浣跨敤瀛楅潰閲?
                    if not col_ref:
                        if expected_type == 'numeric':
                            col_ref = LiteralNode(random.randint(1, 100), 'INT')
                        elif expected_type == 'string':
                            col_ref = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                        elif expected_type == 'datetime':
                            col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                        elif expected_type == 'json':
                            col_ref = LiteralNode('{"type":"Point","coordinates":[0,0]}', 'JSON')
                        elif expected_type == 'binary':
                            col_ref = create_wkb_literal_node(func.name) if func.name.startswith('ST_') and 'FromWKB' in func.name else create_geometry_literal_node(func.name)
                        elif expected_type == 'boolean':
                            col_ref = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                        else:
                            # 榛樿涓烘暟鍊煎瀷
                            col_ref = LiteralNode(random.randint(1, 100), 'INT')

                # 娣诲姞鍙傛暟骞舵鏌ョ被鍨嬫槸鍚﹀尮閰?
                added = func_node.add_child(col_ref)
                if not added:
                    # 濡傛灉绫诲瀷涓嶅尮閰嶏紝灏濊瘯鏌ユ壘鍏朵粬鍖归厤绫诲瀷鐨勫垪
                    found_matching_column = False
                    # 灏濊瘯浠庡彲鐢ㄨ〃涓煡鎵惧尮閰嶇被鍨嬬殑鍒?
                    for _ in range(3):  # 灏濊瘯3娆℃煡鎵惧尮閰嶇被鍨嬬殑鍒?
                        try:
                            # 浠庡彲鐢ㄨ〃涓€夋嫨
                            tables_to_try = available_tables if available_tables else [(main_table, main_alias)]
                            table, alias = random.choice(tables_to_try)
                            # 鑾峰彇涓庨鏈熺被鍨嬪尮閰嶇殑鍒?
                            matching_cols = []
                            for c in table.columns:
                                # 绠€鍗曠殑绫诲瀷鍖归厤閫昏緫
                                if (expected_type == 'numeric' and c.category == 'numeric') or \
                                   (expected_type == 'string' and c.category == 'string') or \
                                   (expected_type == 'datetime' and c.category in ['datetime', 'date', 'timestamp']) or \
                                   (expected_type == 'binary' and c.category == 'binary') or \
                                   (expected_type == 'json' and c.category == 'json') or \
                                   (expected_type == 'boolean' and c.category == 'boolean') or \
                                   expected_type == 'any':
                                    matching_cols.append(c)
                            
                            if matching_cols:
                                matched_col = random.choice(matching_cols)
                                matched_col_ref = ColumnReferenceNode(matched_col, alias)
                                # 鍐嶆灏濊瘯娣诲姞鍖归厤绫诲瀷鐨勫垪
                                if func_node.add_child(matched_col_ref):
                                    found_matching_column = True
                                    break
                        except Exception:
                            # 濡傛灉鏌ユ壘杩囩▼涓嚭閿欙紝缁х画灏濊瘯
                            continue
                    
                    # 濡傛灉鎵句笉鍒板尮閰嶇被鍨嬬殑鍒楋紝鎵嶄娇鐢ㄥ瓧闈㈤噺
                    if not found_matching_column:
                        if expected_type == 'numeric':
                            literal = LiteralNode(random.randint(1, 100), 'INT')
                            func_node.add_child(literal)
                        elif expected_type == 'string':
                            literal = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                            func_node.add_child(literal)
                        elif expected_type == 'datetime':
                            literal = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                            func_node.add_child(literal)
                        elif expected_type == 'json':
                            literal = LiteralNode('{"type":"Point","coordinates":[0,0]}', 'JSON')
                            func_node.add_child(literal)
                        elif expected_type == 'binary':
                            literal = create_wkb_literal_node(func.name) if func.name.startswith('ST_') and 'FromWKB' in func.name else create_geometry_literal_node(func.name)
                            func_node.add_child(literal)
                        elif expected_type == 'boolean':
                            literal = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                            func_node.add_child(literal)
                        else:
                            # 鏈€鍚庡洖閫€鏂规锛屼娇鐢ㄥ師鍒楀紩鐢?
                            func_node.children.append(col_ref)

                # 鐗规畩淇濋殰锛氱‘淇濆嚱鏁版湁瓒冲鐨勫弬鏁?
                # 澶勭悊CONCAT鍑芥暟
                if func.name == 'CONCAT' and len(func_node.children) < func.min_params:
                    # 娣诲姞缂哄け鐨勫瓧绗︿覆瀛楅潰閲忓弬鏁?
                    while len(func_node.children) < func.min_params:
                        literal = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                        func_node.add_child(literal)

                # 濡傛灉鍑芥暟涓嶆槸鑱氬悎鍑芥暟锛屾坊鍔犲垪寮曠敤鍒伴潪鑱氬悎鍒楀垪琛?
                if func.func_type != 'aggregate':
                    if col_ref is not None:
                        non_aggregate_columns.append(col_ref)

            # 浣跨敤澶栭儴鐨剈sed_aliases闆嗗悎杩涜鍘婚噸
            counter = 1
            base_alias = f"col_{counter}"
            current_alias = base_alias
            counter += 1
            # 娣诲姞寰幆淇濇姢鏈哄埗锛岄伩鍏嶆棤闄愬惊鐜?
            max_attempts = 1000
            attempts = 0
            while current_alias in used_aliases and attempts < max_attempts:
                current_alias = f"{base_alias}_{counter}"
                counter += 1
                attempts += 1
            # 濡傛灉杈惧埌鏈€澶у皾璇曟鏁帮紝浣跨敤闅忔満瀛楃涓蹭綔涓哄埆鍚?
            if attempts >= max_attempts:
                current_alias = f"{base_alias}_{random.randint(1000, 9999)}"
            used_aliases.add(current_alias)
            # 鏃犺鑱氬悎鍑芥暟鍙傛暟绫诲瀷濡備綍锛屽彧瑕佹坊鍔犱簡鑱氬悎鍑芥暟灏辫缃爣蹇?
            select_node.add_select_expression(func_node, current_alias)
            if func.func_type == 'aggregate':
                has_aggregate_function = True
            # 纭繚褰撹仛鍚堝嚱鏁板弬鏁版槸瀛愭煡璇㈠垪鏃朵篃鑳芥纭缃爣蹇?
            if func.func_type == 'aggregate' and use_subquery:
                has_aggregate_function = True
        else:  # 鍚﹀垯浣跨敤绠€鍗曞垪
            # 鏍规嵁涓昏〃绫诲瀷閫夋嫨鍒?
            # 鑾峰彇鍒楄窡韪櫒
            column_tracker = select_node.metadata.get('column_tracker')
            
            if use_subquery:
                # 浠庡瓙鏌ヨ鐨勫垪鍒悕涓€夋嫨
                subquery_node = from_node.table_references[0]
                if hasattr(subquery_node, 'column_alias_map'):
                    # 鑾峰彇瀛愭煡璇㈢殑鍒楀埆鍚?
                    valid_aliases = list(subquery_node.column_alias_map.keys())
                    if valid_aliases:
                        # 浣跨敤鍒楄窡韪櫒閫夋嫨鏈娇鐢ㄧ殑鍒?
                        if column_tracker:
                            # 杩囨护鍑烘湭浣跨敤鐨勫垪
                            available_aliases = []
                            for alias in valid_aliases:
                                col_name, data_type, category = subquery_node.column_alias_map[alias]
                                col_identifier = f"{main_alias}.{alias}"
                                if not column_tracker.is_column_used(col_identifier):
                                    available_aliases.append(alias)
                            
                            if available_aliases:
                                alias = random.choice(available_aliases)
                                col_name, data_type, category = subquery_node.column_alias_map[alias]
                                # 鍒涘缓寮曠敤瀛愭煡璇㈠垪鍒悕鐨勫垪寮曠敤
                                col = Column(alias, data_type, category, False, main_alias)
                                # 鏍囪鍒楀凡浣跨敤
                                col_identifier = f"{main_alias}.{alias}"
                                column_tracker.mark_column_as_filter(main_alias, alias)
                                col_ref = ColumnReferenceNode(col, main_alias)
                            else:
                                # 濡傛灉娌℃湁鏈娇鐢ㄧ殑鍒楋紝鍥為€€鍒伴殢鏈洪€夋嫨
                                alias = random.choice(valid_aliases)
                                col_name, data_type, category = subquery_node.column_alias_map[alias]
                                col = Column(alias, data_type, category, False, main_alias)
                                col_ref = ColumnReferenceNode(col, main_alias)
                        else:
                            alias = random.choice(valid_aliases)
                            col_name, data_type, category = subquery_node.column_alias_map[alias]
                            col = Column(alias, data_type, category, False, main_alias)
                            col_ref = ColumnReferenceNode(col, main_alias)
                    else:
                        # 鍥為€€鏂规
                        col = main_table.get_random_column()
                        col_ref = ColumnReferenceNode(col, main_alias)
                else:
                    # 鍥為€€鏂规
                    col = main_table.get_random_column()
                    col_ref = ColumnReferenceNode(col, main_alias)
            else:
                # 浠庢櫘閫氳〃涓€夋嫨鍒?
                # 淇锛氬彧浣跨敤FROM瀛愬彞涓疄闄呭寘鍚殑琛?
                available_tables = []
                for ref in from_node.table_references:
                    if isinstance(ref, Table):
                        available_tables.append((ref, from_node.get_alias_for_table(ref)))
                    elif isinstance(ref, SubqueryNode):
                        available_tables.append((ref, ref.alias))
                
                tables_to_choose_with_aliases = available_tables if available_tables else [(main_table, main_alias)]
                
                # 浣跨敤鍒楄窡韪櫒閫夋嫨鏈娇鐢ㄧ殑鍒?
                if column_tracker:
                    # 杩囨护鍑烘湭浣跨敤鐨勫垪
                    available_columns = []
                    for table, alias in tables_to_choose_with_aliases:
                        for col in table.columns:
                            col_identifier = f"{alias}.{col.name}"
                            if not column_tracker.is_column_used(col_identifier):
                                available_columns.append((table, col, alias))
                    
                    if available_columns:
                        table, col, alias = random.choice(available_columns)
                        # 鏍囪鍒楀凡浣跨敤
                        col_identifier = f"{alias}.{col.name}"
                        column_tracker.mark_column_used(col_identifier)
                        col_ref = ColumnReferenceNode(col, alias)
                    else:
                        # 濡傛灉娌℃湁鏈娇鐢ㄧ殑鍒楋紝鍥為€€鍒伴殢鏈洪€夋嫨
                        table, alias = random.choice(tables_to_choose_with_aliases)
                        col = table.get_random_column()
                        col_ref = ColumnReferenceNode(col, alias)
                else:
                    table, alias = random.choice(tables_to_choose_with_aliases)
                    col = table.get_random_column()
                    col_ref = ColumnReferenceNode(col, alias)
            # 涓虹畝鍗曞垪娣诲姞鍒悕鍘婚噸閫昏緫
            base_alias = col.name
            current_alias = base_alias
            counter = 1
            while current_alias in used_aliases:
                current_alias = f"{base_alias}_{counter}"
                counter += 1
            used_aliases.add(current_alias)
            select_node.add_select_expression(col_ref, current_alias)
            if col_ref is not None:
                non_aggregate_columns.append(col_ref)

    # 闅忔満娣诲姞WHERE瀛愬彞,
    # 纭繚col_ref濮嬬粓琚畾涔?
    if use_subquery:
            # 浠庡瓙鏌ヨ鐨勫垪鍒悕涓€夋嫨
            subquery_node = from_node.table_references[0]
            if hasattr(subquery_node, 'column_alias_map'):
                # 鑾峰彇瀛愭煡璇㈢殑鍒楀埆鍚?
                valid_aliases = list(subquery_node.column_alias_map.keys())
                if valid_aliases:
                    alias = random.choice(valid_aliases)
                    col_name, data_type, category = subquery_node.column_alias_map[alias]
                    # 鍒涘缓寮曠敤瀛愭煡璇㈠垪鍒悕鐨勫垪寮曠敤
                    # 浣跨敤姝ｇ‘鐨勫垪鍚嶅拰鍒悕鍒涘缓瀛愭煡璇㈠垪寮曠敤
                    col = Column(alias, data_type, category, False, main_alias)
                    col_ref = ColumnReferenceNode(col, main_alias)
                else:
                    # 鍥為€€鏂规
                    # 浠嶧ROM瀛愬彞涓疄闄呭寘鍚殑琛ㄤ腑閫夋嫨
                    available_tables = []
                    for ref in from_node.table_references:
                        if isinstance(ref, Table):
                            available_tables.append((ref, from_node.get_alias_for_table(ref)))
                        elif isinstance(ref, SubqueryNode):
                            available_tables.append((ref, ref.alias))
                    
                    tables_to_choose_with_aliases = available_tables if available_tables else [(main_table, main_alias)]
                    
                    table, alias = random.choice(tables_to_choose_with_aliases)
                    col = table.get_random_column()
                    col_ref = ColumnReferenceNode(col, alias)
            else:
                # 鍥為€€鏂规
                # 浠嶧ROM瀛愬彞涓疄闄呭寘鍚殑琛ㄤ腑閫夋嫨
                available_tables = []
                for ref in from_node.table_references:
                    if isinstance(ref, Table):
                        available_tables.append((ref, from_node.get_alias_for_table(ref)))
                    elif isinstance(ref, SubqueryNode):
                        available_tables.append((ref, ref.alias))
                
                tables_to_choose_with_aliases = available_tables if available_tables else [(main_table, main_alias)]
                
                table, alias = random.choice(tables_to_choose_with_aliases)
                col = table.get_random_column()
                col_ref = ColumnReferenceNode(col, alias)
    else:
            # 浠嶧ROM瀛愬彞涓疄闄呭寘鍚殑琛ㄤ腑閫夋嫨
            available_tables = []
            for ref in from_node.table_references:
                if isinstance(ref, Table):
                    available_tables.append((ref, from_node.get_alias_for_table(ref)))
                elif isinstance(ref, SubqueryNode):
                    available_tables.append((ref, ref.alias))
            
            tables_to_choose_with_aliases = available_tables if available_tables else [(main_table, main_alias)]
            
            table, alias = random.choice(tables_to_choose_with_aliases)
            col = table.get_random_column()
            col_ref = ColumnReferenceNode(col, alias)
    # 鍙湁鍦╮andom.random() > 0.2鏃舵墠娣诲姞WHERE瀛愬彞
    if random.random() > 0.2:
        # 浣跨敤create_where_condition鍑芥暟鐢熸垚WHERE鏉′欢锛屾敮鎸佸瓙鏌ヨ绛変赴瀵屾潯浠?
        where_node = create_where_condition(
            tables, functions, from_node, main_table, main_alias,
            join_table if 'join_table' in locals() else None, 
            join_alias if 'join_alias' in locals() else None,
            use_subquery=use_subquery,
            column_tracker=select_node.metadata.get('column_tracker')
        )
        select_node.set_where_clause(where_node)

    # 濡傛灉鏈夎仛鍚堝嚱鏁帮紝鍒欐坊鍔燝ROUP BY瀛愬彞
    # 浣跨敤缁熶竴鐨勬爣蹇梙as_aggregate_function鏉ヨЕ鍙慓ROUP BY鐢熸垚
    if has_aggregate_function:
        group_by = GroupByNode()
        # 鍒濆鍖栧凡娣诲姞鍒楃殑闆嗗悎
        added_columns = set()
        # 棰濆娣诲姞0-1涓殢鏈哄垎缁勫垪锛堝彧鑳芥潵鑷煡璇腑宸叉湁鐨勮〃鍜屽瓙鏌ヨ鐨勮緭鍑哄垪锛?
        additional_groups = random.randint(0, 1)
        available_columns = []

        # 瀵逛簬瀛愭煡璇紝鍙坊鍔犲瓙鏌ヨ杈撳嚭鐨勫垪锛堝垪鍒悕锛?
        if use_subquery and hasattr(from_node.table_references[0], 'column_alias_map'):
            subquery_node = from_node.table_references[0]
            for alias, (col_name, data_type, category) in subquery_node.column_alias_map.items():
                # 鍒涘缓铏氭嫙鍒楄〃绀哄瓙鏌ヨ鐨勮緭鍑哄垪锛屼娇鐢ㄥ埆鍚嶄綔涓哄垪鍚?
                # 浣跨敤姝ｇ‘鐨勫垪鍚嶅拰鍒悕鍒涘缓瀛愭煡璇㈠垪寮曠敤
                col = Column(alias, data_type, category, False, main_alias)
                available_columns.append((col, main_alias))
        else:
            # 娣诲姞涓昏〃鍜岃繛鎺ヨ〃鐨勫垪
            if 'main_table' in locals():
                for col in main_table.columns:
                    available_columns.append((col, main_alias))
            if 'join_table' in locals():
                for col in join_table.columns:
                    available_columns.append((col, join_alias))
        


       
        # 娣诲姞HAVING瀛愬彞
        if random.random() > 0.5:
            # 閫夋嫨涓€涓仛鍚堝嚱鏁?
            agg_funcs = [f for f in functions if f.func_type == "aggregate"]
            if agg_funcs:
                agg_func = random.choice(agg_funcs)
                func_node = FunctionCallNode(agg_func)
                # 涓鸿仛鍚堝嚱鏁伴€夋嫨鍚堥€傜殑鍒?
                # 鑾峰彇鍒楄窡韪櫒
                column_tracker = select_node.metadata.get('column_tracker')
                numeric_required = agg_func.name in [
                    'SUM', 'AVG', 'STD', 'STDDEV', 'STDDEV_POP', 'STDDEV_SAMP',
                    'VARIANCE', 'VAR_SAMP', 'VAR_POP', 'SUM_DISTINCT',
                    'BIT_AND', 'BIT_OR', 'BIT_XOR'
                ]
                param_added = False
                
                # 鏍规嵁鏌ヨ绫诲瀷閫夋嫨鍚堥€傜殑鍒?
                if use_subquery and hasattr(from_node.table_references[0], 'column_alias_map'):
                    subquery_node = from_node.table_references[0]
                    # 浠庡瓙鏌ヨ鐨勮緭鍑哄垪涓€夋嫨
                    valid_aliases = list(subquery_node.column_alias_map.keys())
                    if valid_aliases:
                        candidate_aliases = valid_aliases
                        if numeric_required:
                            candidate_aliases = []
                            for alias in valid_aliases:
                                _, data_type, category = subquery_node.column_alias_map[alias]
                                if normalize_category(category, data_type) == 'numeric':
                                    candidate_aliases.append(alias)
                            if not candidate_aliases:
                                func_node.add_child(LiteralNode(random.randint(1, 100), 'INT'))
                                param_added = True

                        if not param_added:
                            # 浣跨敤鍒楄窡韪櫒閫夋嫨鏈娇鐢ㄧ殑鍒?
                            if column_tracker:
                                # 杩囨护鍑烘湭浣跨敤鐨勫垪
                                available_aliases = []
                                for alias in candidate_aliases:
                                    col_name, data_type, category = subquery_node.column_alias_map[alias]
                                    col = Column(alias, data_type, category, False, main_alias)
                                    col_identifier = f"{main_alias}.{alias}"
                                    if not column_tracker.is_column_used(col_identifier):
                                        available_aliases.append(alias)
                                
                                if available_aliases:
                                    alias = random.choice(available_aliases)
                                    col_name, data_type, category = subquery_node.column_alias_map[alias]
                                    col = Column(alias, data_type, category, False, main_alias)
                                    # 鏍囪鍒楀凡浣跨敤
                                    col_identifier = f"{main_alias}.{alias}"
                                    column_tracker.mark_column_as_filter(main_alias,alias)
                                    func_node.add_child(ColumnReferenceNode(col, main_alias))
                                    param_added = True
                                else:
                                    # 濡傛灉娌℃湁鏈娇鐢ㄧ殑鍒楋紝鍥為€€鍒伴殢鏈洪€夋嫨
                                    alias = random.choice(candidate_aliases)
                                    col_name, data_type, category = subquery_node.column_alias_map[alias]
                                    col = Column(alias, data_type, category, False, main_alias)
                                    func_node.add_child(ColumnReferenceNode(col, main_alias))
                                    param_added = True
                            else:
                                alias = random.choice(candidate_aliases)
                                col_name, data_type, category = subquery_node.column_alias_map[alias]
                                col = Column(alias, data_type, category, False, main_alias)
                                func_node.add_child(ColumnReferenceNode(col, main_alias))
                                param_added = True
                    else:
                        # 鍥為€€鏂规
                        col = main_table.get_random_column()
                        func_node.add_child(ColumnReferenceNode(col, main_alias))
                        param_added = True
                else:
                    # 浠庝富琛ㄩ€夋嫨鍒?
                    # 榛樿鍒濆鍖杤alid_columns鍙橀噺锛岀‘淇濆畠濮嬬粓鏈夊€?
                    valid_columns = main_table.columns
                    
                    if agg_func.name in ['SUM', 'AVG']:
                        valid_columns = [col for col in main_table.columns if col.category == 'numeric']
                    elif agg_func.name in ['MAX', 'MIN']:
                        valid_columns = [col for col in main_table.columns if col.category in ['numeric', 'datetime', 'string']]
                    elif agg_func.name == 'COUNT':
                        valid_columns = main_table.columns
                    else:
                        valid_columns = [col for col in main_table.columns if col.category == 'numeric']
                    
                    # 浣跨敤鍒楄窡韪櫒閫夋嫨鏈娇鐢ㄧ殑鍒?
                    if numeric_required and not valid_columns:
                        func_node.add_child(LiteralNode(random.randint(1, 100), 'INT'))
                        param_added = True
                    if not param_added and column_tracker:
                        # 杩囨护鍑烘湭浣跨敤鐨勫垪
                        available_columns = []
                        for col in valid_columns:
                            col_identifier = f"{main_alias}.{col.name}"
                            if not column_tracker.is_column_used(col_identifier):
                                available_columns.append(col)
                        
                        if available_columns:
                            col = random.choice(available_columns)
                            # 鏍囪鍒楀凡浣跨敤
                            col_identifier = f"{main_alias}.{col.name}"
                            column_tracker.mark_column_as_filter(main_alias, col.name)
                            func_node.add_child(ColumnReferenceNode(col, main_alias))
                            param_added = True
                        else:
                            # 濡傛灉娌℃湁鏈娇鐢ㄧ殑鍒楋紝鍥為€€鍒伴殢鏈洪€夋嫨
                            if valid_columns:
                                col = random.choice(valid_columns)
                            else:
                                col = main_table.get_random_column()
                            func_node.add_child(ColumnReferenceNode(col, main_alias))
                            param_added = True
                    elif not param_added:
                        # 纭繚鑷冲皯鏈変竴涓湁鏁堝垪
                        if not valid_columns:
                            valid_columns = main_table.columns
                if not param_added:
                    if valid_columns:
                        col = random.choice(valid_columns)
                    else:
                        col = main_table.get_random_column()
                    func_node.add_child(ColumnReferenceNode(col, main_alias))
                    param_added = True

            agg_return_category = map_return_type_to_category(agg_func.return_type)
            operators = get_comparison_operators(agg_return_category)
            operators.extend(['IS NULL', 'IS NOT NULL'])
            operator = random.choice(operators)
            having_node = ComparisonNode(operator)
            having_node.add_child(func_node)
            
            if operator not in ['IS NULL', 'IS NOT NULL']:
                if agg_return_category == 'numeric':
                    literal_value = random.randint(0, 10)
                    literal_type = "INT"
                    if agg_func.name == 'COUNT':
                        literal_value = random.randint(0, 5)
                    elif agg_func.name in ['AVG', 'SUM']:
                        literal_type = "DECIMAL"
                        literal_value = round(random.uniform(0, 10), 2)
                    having_node.add_child(LiteralNode(literal_value, literal_type))
                elif agg_return_category == 'datetime':
                    having_node.add_child(LiteralNode('2023-01-01 12:00:00', 'DATETIME'))
                elif agg_return_category == 'string':
                    having_node.add_child(LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING'))
                elif agg_return_category == 'json':
                    having_node.add_child(LiteralNode('{"key": "value"}', 'JSON'))
                elif agg_return_category == 'binary':
                    hex_value = ''.join(random.choices('0123456789ABCDEF', k=8))
                    having_node.add_child(LiteralNode(f"X'{hex_value}'", 'BINARY'))
                elif agg_return_category == 'boolean':
                    having_node.add_child(LiteralNode(random.choice([True, False]), 'BOOLEAN'))
                else:
                    having_node.add_child(LiteralNode(random.randint(0, 10), 'INT'))

            select_node.set_having_clause(having_node)

    # 闅忔満娣诲姞ORDER BY瀛愬彞
    if random.random() > 0.4:
        order_by = OrderByNode()
        # 妫€鏌ユ槸鍚︽湁GROUP BY瀛愬彞
        if has_aggregate_function and select_node.group_by_clause:
            # 鏈塆ROUP BY瀛愬彞锛屽彧鑳介€夋嫨GROUP BY鍒楁垨鑱氬悎鍑芥暟
            valid_order_columns = []
            
            # 娣诲姞GROUP BY鍒?
            if hasattr(select_node.group_by_clause, 'expressions'):
                valid_order_columns.extend(select_node.group_by_clause.expressions)
            
            # 娣诲姞鑱氬悎鍑芥暟
            for expr, _ in select_node.select_expressions:
                if hasattr(expr, 'metadata') and expr.metadata.get('is_aggregate', False):
                    valid_order_columns.append(expr)
            
            # 纭繚鑷冲皯鏈変竴涓湁鏁堢殑ORDER BY鍒?
            if valid_order_columns:
                # 浠庢湁鏁堝垪涓€夋嫨
                expr = random.choice(valid_order_columns)
                order_by.add_expression(expr, random.choice(["ASC", "DESC"]))
                select_node.set_order_by_clause(order_by)
        elif not (has_aggregate_function and select_node.group_by_clause) and use_subquery and hasattr(from_node.table_references[0], 'column_alias_map'):
            # 浠庡瓙鏌ヨ鐨勫垪鍒悕涓€夋嫨
            subquery_node = from_node.table_references[0]
            valid_aliases = list(subquery_node.column_alias_map.keys())
            if valid_aliases:
                alias = random.choice(valid_aliases)
                col_name, data_type, category = subquery_node.column_alias_map[alias]
                # 浣跨敤姝ｇ‘鐨勫垪鍚嶅拰鍒悕鍒涘缓瀛愭煡璇㈠垪寮曠敤
                col = Column(alias, data_type, category, False, main_alias)
                expr = ColumnReferenceNode(col, main_alias)
                
                # 妫€鏌ユ槸鍚︿娇鐢ㄤ簡DISTINCT锛屽鏋滄槸锛屽垯纭繚ORDER BY鐨勫垪鍦⊿ELECT鍒楄〃涓?
                if select_node.distinct:
                    # 妫€鏌ユ槸鍚﹀凡鍦⊿ELECT鍒楄〃涓?
                    in_select = False
                    for selected_expr, selected_alias in select_node.select_expressions:
                        if hasattr(selected_expr, 'to_sql') and selected_expr.to_sql() == expr.to_sql():
                            in_select = True
                            break
                    
                    # 濡傛灉涓嶅湪SELECT鍒楄〃涓紝鍒欐坊鍔?
                    if not in_select:
                        select_node.add_select_expression(expr, alias)
                
                order_by.add_expression(expr, random.choice(["ASC", "DESC"]))
                select_node.set_order_by_clause(order_by)
        elif not (has_aggregate_function and select_node.group_by_clause):
            # 浠庝富琛ㄩ€夋嫨鍒?
            col = main_table.get_random_column()
            expr = ColumnReferenceNode(col, main_alias)
            
            # 妫€鏌ユ槸鍚︿娇鐢ㄤ簡DISTINCT锛屽鏋滄槸锛屽垯纭繚ORDER BY鐨勫垪鍦⊿ELECT鍒楄〃涓?
            if select_node.distinct:
                # 妫€鏌ユ槸鍚﹀凡鍦⊿ELECT鍒楄〃涓?
                in_select = False
                for selected_expr, selected_alias in select_node.select_expressions:
                    if hasattr(selected_expr, 'to_sql') and selected_expr.to_sql() == expr.to_sql():
                        in_select = True
                        break
                
                # 濡傛灉涓嶅湪SELECT鍒楄〃涓紝鍒欐坊鍔?
                if not in_select:
                    select_node.add_select_expression(expr, col.name)
            
            order_by.add_expression(expr, random.choice(["ASC", "DESC"]))
            select_node.set_order_by_clause(order_by)

    # 闅忔満娣诲姞LIMIT瀛愬彞
    if random.random() > 0.5 and not has_aggregate_function:
        select_node.set_limit_clause(LimitNode(random.randint(1, 10)))

    # 闅忔満娣诲姞閿佸畾瀛愬彞 (绾?0%姒傜巼)
    if random.random() > 0.7:
        # 闅忔満閫夋嫨涓€绉嶉攣瀹氭ā寮?
        lock_modes = ['update', 'share', 'no key update', 'key share']
        selected_mode = random.choice(lock_modes)
        select_node.set_for_update(selected_mode)

    # 楠岃瘉骞朵慨澶峉QL
    valid, errors = select_node.validate_all_columns()
    if not valid:
        select_node.repair_invalid_columns()
    # 绠€鍖栫増鍒楀紩鐢ㄥ鐞嗗嚱鏁?- 鍒犻櫎浜嗗垪鍚嶆湁鏁堟€ф楠?
    def validate_column_references(node, available_tables, used_aliases):
        # 瀛愭煡璇娇鐢ㄨ嚜宸辩殑FROM瀛愬彞锛屼笉鑳界敤澶栧眰鍒悕淇
        if isinstance(node, SubqueryNode):
            if hasattr(node, 'repair_columns'):
                node.repair_columns(None)
            return
        # 閫掑綊妫€鏌ュ瓙鑺傜偣
        for child in getattr(node, 'children', []):
            validate_column_references(child, available_tables, used_aliases)

        # 妫€鏌ュ綋鍓嶈妭鐐规槸鍚︽槸鍒楀紩鐢?
        if hasattr(node, 'table_alias') and hasattr(node, 'column'):
            # 浠呬繚鐣欒〃鍒悕瀛樺湪鎬ф鏌ワ紝鍒犻櫎鍒楀悕鏈夋晥鎬ф鏌?
            table_alias = node.table_alias
            if table_alias not in available_tables and available_tables:
                # 灏濊瘯淇锛氫娇鐢ㄧ涓€涓彲鐢ㄨ〃鍒悕
                new_alias = list(available_tables.keys())[0]
                node.table_alias = new_alias

    # 妫€鏌ュ苟淇SQL涓殑鎵€鏈夊嚱鏁板弬鏁?
    def fix_all_function_params(select_node, main_table, join_table=None, main_alias=None, join_alias=None):
    # 鏋勫缓鍙敤琛ㄥ瓧鍏?
        available_tables = {}
        if main_table and main_alias:
            available_tables[main_alias] = main_table
        if join_table and join_alias:
            available_tables[join_alias] = join_table

        # 楠岃瘉骞朵慨澶嶅垪寮曠敤锛堝寘鎷琌N瀛愬彞锛?
        validate_column_references(select_node, available_tables, {})

        # 鐗瑰埆妫€鏌ュ苟淇ON瀛愬彞涓殑鍒楀紩鐢?
        if hasattr(select_node, 'from_clause') and hasattr(select_node.from_clause, 'joins'):
            for join in select_node.from_clause.joins:
                condition = join.get('condition')
                if condition:
                    validate_column_references(condition, available_tables, {})


        # 鍒犻櫎浜嗛噸澶嶅垪鍒悕妫€鏌ラ€昏緫

    # 纭繚GROUP BY瀛愬彞鍖呭惈鎵€鏈塖ELECT涓殑闈炶仛鍚堝垪 - 绗﹀悎only_full_group_by妯?
    # 濡傛灉鏈夎仛鍚堝嚱鏁帮紝蹇呴』纭繚绗﹀悎only_full_group_by妯″紡
    # 缁熶竴浣跨敤has_aggregate_function鏍囧織
    if has_aggregate_function:
        # 妫€鏌ELECT鍒楄〃涓槸鍚﹀彧鏈夎仛鍚堝嚱鏁?
        all_agg = all(hasattr(expr, 'function') and expr.function.func_type == 'aggregate' for expr, _ in select_node.select_expressions)
        
        if not all_agg:
            # 鎯呭喌1: 鏈夎仛鍚堝嚱鏁颁笖鏈夐潪鑱氬悎鍒?- 蹇呴』鏈塆ROUP BY瀛愬彞
            if not hasattr(select_node, 'group_by_clause') or not select_node.group_by_clause:
                # 鍒涘缓GROUP BY瀛愬彞
                select_node.group_by_clause = GroupByNode()
                
    # 鐜板湪纭繚GROUP BY瀛愬彞鍖呭惈鎵€鏈夊繀瑕佺殑鍒?
    group_by = getattr(select_node, 'group_by_clause', None)
    if group_by:
        group_by.expressions = []
        for expr, alias in select_node.select_expressions:
            if hasattr(expr, 'function') and expr.function.func_type == 'aggregate':
                continue
            if hasattr(expr, 'function') and expr.function.func_type == 'scalar':
                before_count = len(group_by.expressions)
                _add_group_by_from_scalar(group_by, expr)
                after_count = len(group_by.expressions)
            elif type(expr).__name__ == 'ColumnReferenceNode':
                before_count = len(group_by.expressions)
                group_by.add_expression(expr)
                after_count = len(group_by.expressions)

            if hasattr(expr, 'function') and expr.function.func_type == 'window':
                if hasattr(expr, 'children'):
                    for arg in expr.children:
                        if type(arg).__name__ == 'ColumnReferenceNode':
                            group_by.add_expression(arg)
                        elif type(arg).__name__ == 'FunctionCallNode' and hasattr(arg.function, 'func_type') and arg.function.func_type == 'scalar':
                            for child_arg in arg.children:
                                if type(child_arg).__name__ == 'ColumnReferenceNode':
                                    group_by.add_expression(child_arg)

                if hasattr(expr, 'metadata'):
                    if expr.metadata.get('partition_by'):
                        partition_by = expr.metadata.get('partition_by')
                        before_count = len(group_by.expressions)
                        if 'from_node' in locals() or 'from_node' in globals():
                            available_from_node = locals().get('from_node') or globals().get('from_node')
                            for part_expr in partition_by:
                                try:
                                    if '.' in part_expr:
                                        alias_part, col_part = part_expr.split('.', 1)
                                        alias_part = alias_part.strip('"\'')
                                        col_part = col_part.strip('"\'')
                                        table_ref = available_from_node.get_table_for_alias(alias_part)
                                        if table_ref and hasattr(table_ref, 'get_column'):
                                            col = table_ref.get_column(col_part)
                                            if col:
                                                col_ref = ColumnReferenceNode(col, alias_part)
                                                group_by.add_expression(col_ref)
                                except Exception as e:
                                    print(f"  杞崲partition_by琛ㄨ揪寮忔椂鍑洪敊: {e}")
                        after_count = len(group_by.expressions)

                    if expr.metadata.get('order_by'):
                        order_by = expr.metadata.get('order_by')
                        before_count = len(group_by.expressions)
                        for order in order_by:
                            expr_parts = order.rsplit(' ', 1)
                        if len(expr_parts) == 2 and expr_parts[1].upper() in ['ASC', 'DESC']:
                            main_expr = [expr_parts[0]]
                        else:
                            main_expr = order_by
                        if 'from_node' in locals() or 'from_node' in globals():
                            available_from_node = locals().get('from_node') or globals().get('from_node')
                            for part_expr in main_expr:
                                try:
                                    if '.' in part_expr:
                                        alias_part, col_part = part_expr.split('.', 1)
                                        alias_part = alias_part.strip('"\'')
                                        col_part = col_part.strip('"\'')
                                        table_ref = available_from_node.get_table_for_alias(alias_part)
                                        if table_ref and hasattr(table_ref, 'get_column'):
                                            col = table_ref.get_column(col_part)
                                            if col:
                                                col_ref = ColumnReferenceNode(col, alias_part)
                                                group_by.add_expression(col_ref)
                                        else:
                                            virtual_col = Column(col_part, 'subquery', 'VARCHAR(255)', False, alias_part)
                                            virtual_col_ref = ColumnReferenceNode(virtual_col, alias_part)
                                            group_by.add_expression(virtual_col_ref)
                                    else:
                                        print(f"  璀﹀憡: 鏃犳硶瑙ｆ瀽order_by琛ㄨ揪寮忄牸寮? {part_expr}")
                                except Exception as e:
                                    print(f"  杞崲order_by琛ㄨ揪寮忄椂鍑洪敊: {e}")
                        else:
                            print("  璀﹀憡: from_node瀵硅薄涓嶅彲鐢紝鏃犳硶灏唎rder_by杞崲涓篊olumnReferenceNode")
                        after_count = len(group_by.expressions)

            if hasattr(select_node, 'order_by_clause') and select_node.order_by_clause:
                for expr, direction in select_node.order_by_clause.expressions:
                    expr = [expr.to_sql()]
                    if 'from_node' in locals() or 'from_node' in globals():
                        available_from_node = locals().get('from_node') or globals().get('from_node')
                        for part_expr in expr:
                            try:
                                if '.' in part_expr:
                                    alias_part, col_part = part_expr.split('.', 1)
                                    alias_part = alias_part.strip('"\'')
                                    col_part = col_part.strip('"\'')
                                    table_ref = available_from_node.get_table_for_alias(alias_part)
                                    if table_ref and hasattr(table_ref, 'get_column'):
                                        col = table_ref.get_column(col_part)
                                        if col:
                                            col_ref = ColumnReferenceNode(col, alias_part)
                                            group_by.add_expression(col_ref)
                                    else:
                                        virtual_col = Column(col_part, 'subquery', 'VARCHAR(255)', False, alias_part)
                                        virtual_col_ref = ColumnReferenceNode(virtual_col, alias_part)
                                        group_by.add_expression(virtual_col_ref)
                                else:
                                    print(f"  璀﹀憡: 鏃犳硶瑙ｆ瀽order_by琛ㄨ揪寮忄牸寮? {part_expr}")
                            except Exception as e:
                                print(f"  杞崲order_by琛ㄨ揪寮忄椂鍑洪敊: {e}")
                    else:
                        print("  璀﹀憡: from_node瀵硅薄涓嶅彲鐢紝鏃犳硶灏唎rder_by杞崲涓篊olumnReferenceNode")
    # 鏌ヨ鐢熸垚鍚庯紝妫€鏌ュ苟淇鎵€鏈夊嚱鏁板弬鏁?
    fix_all_function_params(select_node, main_table, join_table if 'join_table' in locals() else None, main_alias, join_alias if 'join_alias' in locals() else None)
    # 鎵ц鍑芥暟鍙傛暟淇
    try:
        fix_all_function_params(select_node, main_table, join_table, main_alias, join_alias)
    except Exception as e:
        pass
    if use_cte:
        return f'{with_node.to_sql()} {select_node.to_sql()}'
    else:
        return select_node.to_sql()

def Generate(subquery_depth: int = 3, total_insert_statements: int = 100, num_queries: int = 15, query_type: str = 'default', use_database_tables: bool = False, db_config: Optional[Dict] = None):
    """涓诲嚱鏁帮細鐢熸垚骞朵繚瀛楽QL璇彞锛堝缓琛ㄣ€佹彃鍏ュ拰鏌ヨ锛?

    鍙傛暟:
    - subquery_depth: 瀛愭煡璇㈢殑鏈€澶ф繁搴?
    - total_insert_statements: 鐢熸垚鐨勬彃鍏ヨ鍙ユ€绘暟
    - num_queries: 鐢熸垚鐨勬煡璇㈣鍙ユ暟閲?
    - query_type: 鏌ヨ绫诲瀷锛?default'浣跨敤generate_random_sql()锛?aggregate'浣跨敤generate_random_sql_with_aggregate()
    - use_database_tables: 鏄惁浣跨敤鏁版嵁搴撲腑鐨勮〃缁撴瀯锛岄粯璁や负False
    - db_config: 鏁版嵁搴撹繛鎺ラ厤缃紝褰搖se_database_tables涓篢rue鏃跺繀椤绘彁渚?
    """
    
    # 閫夋嫨琛ㄧ粨鏋勬潵婧?    resolved_db_config = db_config or {}
    if use_database_tables:
        required_keys = ["host", "port", "database", "user", "password", "dialect"]
        missing_keys = [k for k in required_keys if resolved_db_config.get(k) in (None, "")]
        if missing_keys:
            raise ValueError(
                "use_database_tables=True requires db_config keys: "
                f"{', '.join(required_keys)}; missing: {', '.join(missing_keys)}"
            )

        # 瀵煎叆DatabaseMetadataFetcher
        from database_metadata_fetcher import DatabaseMetadataFetcher

        # 创建数据库元数据获取器
        fetcher = DatabaseMetadataFetcher(
            host=resolved_db_config["host"],
            port=resolved_db_config["port"],
            database=resolved_db_config["database"],
            user=resolved_db_config["user"],
            password=resolved_db_config["password"],
            dialect=resolved_db_config["dialect"],
        )
        
        # 杩炴帴鍒版暟鎹簱骞惰幏鍙栬〃淇℃伅
        if fetcher.connect():
            tables = fetcher.get_all_tables_info()
            fetcher.disconnect()
            print(f"Loaded {len(tables)} tables from database metadata")
            
            # 灏嗚〃淇℃伅淇濆瓨涓篗arkdown鏂囦欢
            if tables:
                # 纭繚generated_sql鐩綍瀛樺湪
                os.makedirs("generated_sql", exist_ok=True)
                md_file_path = os.path.join("generated_sql", "database_tables_info.md")
                
                try:
                    with open(md_file_path, 'w', encoding='utf-8') as md_file:
                        md_file.write("# 鏁版嵁搴撹〃缁撴瀯淇℃伅\n")
                        md_file.write(f"\n## 鏁版嵁搴撹繛鎺ヤ俊鎭痋n")
                        md_file.write(f"- Host: {resolved_db_config['host']}\n")
                        md_file.write(f"- Port: {resolved_db_config['port']}\n")
                        md_file.write(f"- Database: {resolved_db_config['database']}\n")
                        md_file.write(f"- Dialect: {resolved_db_config['dialect']}\n")
                        md_file.write(f"- Table count: {len(tables)}\n")
                        md_file.write("\n## Tables\n\n")
                        for table in tables:
                            md_file.write(f"### {table.name}\n")
                            primary_key = table.primary_key if table.primary_key else 'N/A'
                            md_file.write(f"- Primary key: {primary_key}\n")
                            md_file.write("\n| Column | Data Type | Category | Nullable |\n")
                            md_file.write("|---|---|---|---|\n")
                            for col in table.columns:
                                md_file.write(f"| {col.name} | {col.data_type} | {col.category} | {'YES' if col.is_nullable else 'NO'} |\n")
                            if table.foreign_keys:
                                md_file.write("\n#### Foreign Keys\n")
                                for fk in table.foreign_keys:
                                    md_file.write(f"- `{fk['column']}` -> `{fk['ref_table']}.{fk['ref_column']}`\n")
                            md_file.write("\n")
                    
                    print(f"Database table metadata saved to: {md_file_path}")
                except Exception as e:
                    print(f"Failed to save database table metadata: {e}")
        else:
            print("Database connection failed, using sample tables")
            tables = create_sample_tables()
            tables = create_sample_tables()
    else:
        # 浣跨敤绀轰緥琛ㄧ粨鏋?
        tables = create_sample_tables()
    
    functions = create_sample_functions()
    _set_subquery_depth(subquery_depth)
    
    # 璁剧疆鍏ㄥ眬琛ㄧ粨鏋勪俊鎭?
    set_tables(tables)
    set_tables(tables)
    
    # 璁板綍鏄惁浣跨敤鏁版嵁搴撹〃
    is_using_database_tables = use_database_tables and len(tables) > 0 and tables[0].name != "users"
    
    
    # 鍙湁鍦ㄤ笉浣跨敤鏁版嵁搴撹〃鏃舵墠鐢熸垚寤鸿〃鍜屾彃鍏ヨ鍙?
    if not is_using_database_tables:
        # 鐢熸垚寤鸿〃璇彞
        create_sqls = []
        for table in tables:
            create_sql = generate_create_table_sql(table)
            create_sqls.append(create_sql)

        # 鐢熸垚鎻掑叆璇彞
        insert_sqls = []
        # 瀛樺偍姣忎釜琛ㄧ殑涓婚敭鍊硷紝鐢ㄤ簬澶栭敭寮曠敤
        primary_keys_dict = {}
        # 璁＄畻姣忎釜琛ㄧ殑鎻掑叆琛屾暟锛堟€绘彃鍏ヨ鍙ユ暟骞冲潎鍒嗛厤鍒板悇涓〃锛?
        num_tables = len(tables)
        insert_rows_per_table = total_insert_statements // num_tables
        remainder = total_insert_statements % num_tables

        # 瀛樺偍姣忎釜琛ㄧ殑鎻掑叆琛屾暟
        table_insert_rows = {}

        # 棣栧厛鐢熸垚鎵€鏈夎〃鐨勪富閿€?
        for i, table in enumerate(tables):
            # 涓鸿〃鐢熸垚涓婚敭鍊?
            primary_key_values = set()
            # 鍒嗛厤鎻掑叆琛屾暟锛屽墠remainder涓〃澶氬垎閰?琛?
            num_rows = insert_rows_per_table + (1 if i < remainder else 0)
            table_insert_rows[table.name] = num_rows
            for _ in range(num_rows):
                while True:
                    val = random.randint(1, 10000)
                    if val not in primary_key_values:
                        primary_key_values.add(val)
                        break
            primary_keys_dict[table.name] = list(primary_key_values)

        # 鎸夌収姝ｇ‘鐨勯『搴忕敓鎴愭彃鍏ヨ鍙ワ紙鍏堟彃鍏ヨ寮曠敤琛級
        # 杩欓噷浣跨敤绠€鍗曠殑鎷撴墤鎺掑簭鏉ョ‘瀹氳〃鐨勬彃鍏ラ『搴?
        visited = set()
        def topological_sort(table_name):
            if table_name in visited:
                return
            visited.add(table_name)
            table = next(t for t in tables if t.name == table_name)
            # 鍏堝鐞嗘墍鏈夎寮曠敤鐨勮〃
            for fk in table.foreign_keys:
                ref_table = fk["ref_table"]
                if ref_table not in visited:
                    topological_sort(ref_table)
            # 鐢熸垚褰撳墠琛ㄧ殑鎻掑叆璇彞
            num_rows = table_insert_rows[table.name]
            insert_sql = generate_insert_sql(table, num_rows=num_rows, existing_primary_keys=primary_keys_dict, primary_key_values=primary_keys_dict[table.name])
            insert_sqls.append(insert_sql)

        # 瀵规墍鏈夎〃杩涜鎷撴墤鎺掑簭骞剁敓鎴愭彃鍏ヨ鍙?
        for table in tables:
            if table.name not in visited:
                topological_sort(table.name)

        # 鐢熸垚绱㈠紩璇彞
        from data_structures.db_dialect import get_current_dialect
        dialect = get_current_dialect()
        index_sqls = generate_index_sqls(tables, dialect)
        
        # 缁勫悎寤鸿〃銆佹彃鍏ュ拰绱㈠紩璇彞
        schema_sql = "\n\n".join(create_sqls + insert_sqls + index_sqls)
        schema_filepath = save_sql_to_file(schema_sql, file_type="schema")
    else:
        # 浣跨敤鏁版嵁搴撹〃鏃讹紝涓嶇敓鎴愬缓琛ㄥ拰鎻掑叆璇彞
        print("浣跨敤鏁版嵁搴撹〃缁撴瀯锛岃烦杩囧缓琛ㄥ拰鎻掑叆璇彞鐢熸垚")
        schema_filepath = "[浣跨敤鏁版嵁搴撹〃锛屾湭鐢熸垚schema鏂囦欢]"
    
    # 鍒嗘壒鐢熸垚骞跺啓鍏ユ煡璇㈣鍙ワ紝闄嶄綆鍐呭瓨鍗犵敤
    batch_size = 1000  # 姣忔壒澶勭悊鐨勬煡璇㈡暟閲?
    query_filepath = save_sql_to_file("", file_type="query")  # 鍒涘缓绌烘枃浠跺苟鍐欏叆USE test;
    
    # 灏嗙储寮昐QL淇濆瓨鍒版煡璇㈡枃浠朵腑
    if not is_using_database_tables:
        # 鐢熸垚绱㈠紩璇彞锛堝鏋滆繕娌℃湁鐢熸垚锛?
        if 'index_sqls' not in locals():
            from data_structures.db_dialect import get_current_dialect
            dialect = get_current_dialect()
            index_sqls = generate_index_sqls(tables, dialect)
        # 灏嗙储寮昐QL鍐欏叆鏌ヨ鏂囦欢
        if index_sqls:
            index_sql_content = "\n\n".join(index_sqls)
            # 娣诲姞鍒嗛殧绗?
            
            # 鍚屾椂灏嗙储寮昐QL淇濆瓨鍒癷ndexes.sql鏂囦欢
            # 鍒涘缓indexes.sql鏂囦欢璺緞
            indexes_file_path = os.path.join("generated_sql", "indexes.sql")
            # 鍐欏叆USE test璇彞鍜岀储寮昐QL
            with open(indexes_file_path, "a", encoding="utf-8") as f:
                # 鑾峰彇褰撳墠鏁版嵁搴撴柟瑷€
                from data_structures.db_dialect import get_current_dialect
                dialect = get_current_dialect()
                # 鍐欏叆USE璇彞
                use_db_sql = dialect.get_use_database_sql("test")
                if use_db_sql:
                    f.write(use_db_sql)
                    if not use_db_sql.endswith("\n"):
                        f.write("\n")
                # 鍐欏叆绱㈠紩SQL
                f.write(index_sql_content)
    
    # 閿欒鏃ュ織鏂囦欢璺緞
    error_log_path = os.path.join("generated_sql", "query_generation_errors.log")
    
    # 鎵撳紑閿欒鏃ュ織鏂囦欢鐢ㄤ簬鍐欏叆
    with open(error_log_path, "w", encoding="utf-8") as error_log:
        error_log.write(f"# SQL鏌ヨ鐢熸垚閿欒鏃ュ織 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        error_log.write(f"# 鐩爣鏌ヨ鏁伴噺: {num_queries}\n")
        error_log.write("\n")
    
    for i in range(0, num_queries, batch_size):
        # 璁＄畻褰撳墠鎵规鐨勬煡璇㈡暟閲?
        current_batch_size = min(batch_size, num_queries - i)
        
        # 鐢熸垚褰撳墠鎵规鐨勬煡璇㈣鍙?
        batch_queries = []
        # 缁熻淇℃伅
        success_count = 0
        fail_count = 0
        error_types = {}
        
        
        for j in range(current_batch_size):
            retry_count = 0
            max_retries = 10  # 澧炲姞鏈€澶ч噸璇曟鏁帮紝鎻愰珮鎴愬姛鐜?
            success = False
            error_info = None
            error_type = None
            
            # 鍒濆鍖栭敊璇浉鍏冲彉閲忥紝閬垮厤鏈畾涔夐敊璇?
            error_type = "UnknownError"
            error_message = ""
            e = Exception("杈惧埌鏈€澶ч噸璇曟鏁颁絾鏈崟鑾峰埌鍏蜂綋寮傚父")
            
            while retry_count < max_retries:
                try:
                    sql = generate_random_sql(tables, functions)
                    batch_queries.append(sql)
                    success = True
                    success_count += 1
                    
                    # 姣忕敓鎴?00鏉℃煡璇紝鎵撳嵃涓€娆℃棩蹇?
                    if (i + j + 1) % 100 == 0:
                        pass
                    break  # 鐢熸垚鎴愬姛锛岃烦鍑洪噸璇曞惊鐜?
                except Exception as e:
                    retry_count += 1
                    # 璁板綍閿欒淇℃伅锛屼絾涓嶇珛鍗虫洿鏂伴敊璇被鍨嬬粺璁?
                    error_type = type(e).__name__
                    error_message = str(e)[:100]  # 闄愬埗閿欒淇℃伅闀垮害
                    error_info = f"{error_type}: {error_message}"
                    
                    # 姣忛噸璇?娆℃墦鍗颁竴娆℃棩蹇楋紝閬垮厤鏃ュ織杩囧
                    # 宸茬Щ闄ら敊璇噸璇曟墦鍗?
            
            if not success:
                fail_count += 1
                
                # 鍙湪鏌ヨ鏈€缁堝け璐ユ椂鏇存柊閿欒绫诲瀷缁熻锛堟瘡涓け璐ユ煡璇㈠彧缁熻涓€娆★級
                error_types[error_type] = error_types.get(error_type, 0) + 1
                
                # 鍐欏叆閿欒鏃ュ織锛屽寘鍚缁嗛敊璇俊鎭?
                with open(error_log_path, "a", encoding="utf-8") as error_log:
                    error_log.write(f"[{i+j+1}/{num_queries}] 鐢熸垚澶辫触:\n")
                    error_log.write(f"  閿欒绫诲瀷: {error_type}\n")
                    error_log.write(f"  閿欒淇℃伅: {error_message}\n")
                    error_log.write(f"  閲嶈瘯娆℃暟: {max_retries}\n")
                    error_log.write("\n")
            
            # 姣忕敓鎴?00鏉℃煡璇紝鎵撳嵃涓€娆¤繘搴﹀拰缁熻淇℃伅
            if (i + j + 1) % 100 == 0:
                pass
            
            # 杩涘害鎻愮ず
            if (i + j + 1) % 5000 == 0:
                print(f"Generated {i + j + 1}/{num_queries} queries")
        
        # 灏嗗綋鍓嶆壒娆＄殑鏌ヨ璇彞鍐欏叆鏂囦欢
        batch_sql = "\n\n".join(batch_queries)
        save_sql_to_file(batch_sql, file_type="query", mode="a")
        
        # 璁板綍鎵规瀹屾垚淇℃伅鍒伴敊璇棩蹇?
        with open(error_log_path, "a", encoding="utf-8") as error_log:
            error_log.write(f"=== 鎵规 {i//batch_size + 1} 瀹屾垚 ===\n")
            error_log.write(f"鎵规鏌ヨ鎬绘暟: {current_batch_size}\n")
            error_log.write(f"鎴愬姛鐢熸垚: {success_count}\n")
            error_log.write(f"鐢熸垚澶辫触: {fail_count}\n")
            error_log.write(f"閿欒绫诲瀷缁熻: {error_types}\n")
            error_log.write("\n")
        
        # 鎵撳嵃鎵规瀹屾垚淇℃伅
        print(f"=== 鎵规 {i//batch_size + 1} 瀹屾垚 ===")
        print(f"鎵规鏌ヨ鎬绘暟: {current_batch_size}")
        print(f"鎴愬姛鐢熸垚: {success_count}")
        print(f"鐢熸垚澶辫触: {fail_count}")
        print(f"閿欒绫诲瀷缁熻: {error_types}")
        
        # 濡傛灉涓嶆槸鏈€鍚庝竴鎵癸紝娣诲姞鍒嗛殧绗?
        if i + current_batch_size < num_queries:
            pass
        # 娓呯┖鎵规鍒楄〃锛岄噴鏀惧唴瀛?
        batch_queries = []
    # 璁板綍鏈€缁堢粺璁′俊鎭埌閿欒鏃ュ織
    with open(error_log_path, "a", encoding="utf-8") as error_log:
        total_success = len([line for line in open(query_filepath, 'r', encoding='utf-8') if line.strip()])
        total_fail = num_queries - total_success
        error_log.write("=== 鏈€缁堢粺璁′俊鎭?===\n")
        error_log.write(f"鐩爣鏌ヨ鏁伴噺: {num_queries}\n")
        error_log.write(f"鎴愬姛鐢熸垚: {total_success}\n")
        error_log.write(f"鐢熸垚澶辫触: {total_fail}\n")
        error_log.write(f"鐢熸垚鐜? {(total_success/num_queries)*100:.2f}%\n")
        error_log.write(f"\n鐢熸垚鏃ュ織鍒涘缓鏃堕棿: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # 鎵撳嵃鏈€缁堢粨鏋滀俊鎭?
    if not is_using_database_tables:
        print(f"Schema SQL宸蹭繚瀛樺埌: {schema_filepath}")
    else:
        print("浣跨敤鏁版嵁搴撹〃缁撴瀯锛屾湭鐢熸垚schema鏂囦欢")
    print(f"Query SQL宸蹭繚瀛樺埌: {query_filepath}")
    print(f"閿欒鏃ュ織宸蹭繚瀛樺埌: {error_log_path}")
    print(f"鐩爣鏌ヨ鏁伴噺: {num_queries}")
    print(f"瀹為檯鐢熸垚鏁伴噺: {len(batch_queries) + (i if i > 0 else 0)}")
    print(f"鐢熸垚鐜? {(len(batch_queries) + (i if i > 0 else 0))/num_queries*100:.2f}%")


