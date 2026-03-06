import random
from typing import Optional, Any
from .column import Column

class ColumnUsageTracker:
    """跟踪SQL查询中列的使用情况"""
    
    def __init__(self):
        # 存储所有已使用的列标识符
        self.used_columns = set()
        # 存储在select子句中使用的列标识符
        self.select_columns = set()
        # 存储在filter子句(where/having/on)中使用的列标识符
        self.filter_columns = set()
        # 存储所有可用的列（表别名 -> 列列表）
        self.available_columns = {}
        # 存储所有表引用信息（表别名 -> 表对象）
        self.table_references = {}
        
    def initialize_from_from_node(self, from_node):
        """根据FROM子句中的表或子查询初始化可用列信息
        
        Args:
            from_node: FromNode对象，包含表引用和连接信息
        """
        print(f"[ColumnTracker] 从FROM子句初始化可用列信息")
        
        # 清空现有信息
        self.available_columns = {}
        self.table_references = {}
        
        # 获取所有表引用
        if hasattr(from_node, 'table_references') and hasattr(from_node, 'aliases'):
            # 遍历所有表引用和别名
            for table_ref, alias in zip(from_node.table_references, from_node.aliases):
                self.table_references[alias] = table_ref
    
    def select_column_for_select(self, table, table_alias):
        """为select子句选择列（允许重复）"""
        if hasattr(table, 'columns') and table.columns:
            return random.choice(table.columns)
        return None
    
    def select_column_for_filter(self, table, table_alias):
        """为filter子句选择列（不在select和filter中）"""
        if hasattr(table, 'columns') and table.columns:
            # 优先选择未在任何地方使用的列
            unused_columns = [col for col in table.columns if f"{table_alias}.{col.name}" not in self.used_columns]
            if unused_columns:
                return random.choice(unused_columns)
            # 如果没有未使用的列，选择仅在select中使用的列
            select_only_columns = [col for col in table.columns if f"{table_alias}.{col.name}" in self.select_columns and f"{table_alias}.{col.name}" not in self.filter_columns]
            if select_only_columns:
                return random.choice(select_only_columns)
        return None
    
    def mark_column_as_select(self, table_alias, column_name):
        """标记列在select子句中使用"""
        col_id = f"{table_alias}.{column_name}"
        self.used_columns.add(col_id)
        self.select_columns.add(col_id)
    
    def mark_column_as_filter(self, table_alias, column_name):
        """标记列在filter子句中使用"""
        col_id = f"{table_alias}.{column_name}"
        self.used_columns.add(col_id)
        self.filter_columns.add(col_id)

# 修改get_random_column方法以支持列使用跟踪器
def get_random_column_with_tracker(table: Any, table_alias: str, column_tracker: ColumnUsageTracker = None, for_select: bool = False) -> Optional[Any]:
    """根据列使用跟踪器选择列
    table: 表对象
    table_alias: 表别名
    column_tracker: 列跟踪器实例
    for_select: 是否为select子句选择列
    """
    print(f"[ColumnTracker] 调用get_random_column_with_tracker - 表: {table_alias}, for_select: {for_select}")
    
    if column_tracker:
        print(f"[ColumnTracker] 使用列跟踪器选择列")
        
        if for_select:
            # 为select子句选择列（允许重复）
            col = column_tracker.select_column_for_select(table, table_alias)
            if col:
                print(f"[ColumnTracker] 成功选择列用于select子句: {table_alias}.{col.name}")
                column_tracker.mark_column_as_select(table_alias, col.name)
                return col
        else:
            # 为filter子句选择列（不在select和filter中）
            col = column_tracker.select_column_for_filter(table, table_alias)
            if col:
                print(f"[ColumnTracker] 成功选择列用于filter子句: {table_alias}.{col.name}")
                column_tracker.mark_column_as_filter(table_alias, col.name)
                return col
            
            # 如果没有可用于filter的列，使用回退方案（选择任意列）
            print(f"[ColumnTracker] 警告: 没有可用于filter的列，使用回退方案")
            
            if hasattr(table, 'columns') and table.columns:
                selected_col = random.choice(table.columns)
                column_tracker.mark_column_as_filter(table_alias, selected_col.name)
                return selected_col
            elif hasattr(table, 'column_alias_map'):
                valid_aliases = list(table.column_alias_map.keys())
                if valid_aliases:
                    alias = random.choice(valid_aliases)
                    col_name, data_type, category = table.column_alias_map[alias]
                    column_tracker.mark_column_as_filter(table_alias, alias)
                    return Column(alias, data_type, category, False, table_alias)
        
        return None
    
    # 如果没有跟踪器，使用原始逻辑
    if hasattr(table, 'get_random_column'):
        return table.get_random_column()
    elif hasattr(table, 'columns') and table.columns:
        return random.choice(table.columns)
    elif hasattr(table, 'column_alias_map'):
        valid_aliases = list(table.column_alias_map.keys())
        if valid_aliases:
            alias = random.choice(valid_aliases)
            col_name, data_type, category = table.column_alias_map[alias]
            return Column(alias, data_type, category, False, table_alias)
    
    return None