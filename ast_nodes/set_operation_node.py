# SetOperationNode类定义 - 集合操作节点
from typing import List, Set
from .ast_node import ASTNode
from data_structures.node_type import NodeType

class SetOperationNode(ASTNode):
    """集合操作节点（UNION/UNION ALL/INTERSECT/EXCEPT）"""

    def __init__(self, operation_type: str):
        super().__init__(NodeType.SET_OPERATION)
        self.operation_type = operation_type  # 'UNION', 'UNION ALL', 'INTERSECT', 'EXCEPT'
        self.queries: List['SelectNode'] = []  # 参与集合操作的SELECT查询

    def add_query(self, select_node: 'SelectNode') -> None:
        """添加参与集合操作的SELECT查询"""
        self.queries.append(select_node)
        self.add_child(select_node)

    def to_sql(self) -> str:
        """转换为SQL字符串"""
        if not self.queries or len(self.queries) < 2:
            return ""  # 至少需要两个查询
        
        # 获取当前数据库方言
        from data_structures.db_dialect import get_current_dialect
        current_dialect = get_current_dialect()
        #print(f"#{current_dialect.__class__.__name__}")
        is_polardb = current_dialect.__class__.__name__ == 'PolarDBDialect'
        # 检查是否为Percona方言
        is_percona = 'percona' in current_dialect.name.lower() or (hasattr(current_dialect, '__class__') and 'percona' in current_dialect.__class__.__name__.lower())
        
        # 确保每个查询都是有效的SQL语句
        query_sqls = []
        for query in self.queries:
            sql = query.to_sql()
            # 移除可能导致问题的额外空格或换行符
            sql = sql.strip()
            # 确保每个查询都不是空字符串
            if sql:
                # 检查查询是否包含ORDER BY或LIMIT子句，如果有则用括号括起来
                # 对于PolarDB，左查询（第一个查询）不使用括号包裹
                if not is_polardb:
                    if ('ORDER BY' in sql.upper() or 'LIMIT' in sql.upper()) and not is_polardb:
                        # 确保查询没有被括号包围
                        if not (sql.startswith('(') and sql.endswith(')')):
                            sql = f"({sql})"
                query_sqls.append(sql)
        
        # 如果没有有效的查询，返回空字符串
        if len(query_sqls) < 2:
            return ""
        
        # 为Percona方言特殊处理：Percona 5.7不支持INTERSECT和EXCEPT操作符
        if is_percona:
            if self.operation_type in ['INTERSECT', 'EXCEPT']:
                # 在Percona环境下，将INTERSECT和EXCEPT转换为UNION ALL
                # 注意：这不是语义等价的转换，但可以避免语法错误
                # 实际应用中可能需要更复杂的转换策略
                return " UNION ALL ".join(query_sqls).strip()
            elif self.operation_type == "UNION":
                # Percona支持UNION但我们统一使用UNION ALL以避免潜在问题
                return " UNION ALL ".join(query_sqls).strip()
            else:
                # 对于UNION ALL，保持原样
                return " UNION ALL ".join(query_sqls).strip()
        else:
            # 非Percona方言的正常处理
            if self.operation_type == "UNION ALL":
                # 确保操作符前后只有一个空格，并且没有多余的空格或换行符
                return " UNION ALL ".join(query_sqls).strip()
            else:
                # 处理其他操作符
                return f" {self.operation_type} ".join(query_sqls).strip()

    def contains_window_function(self) -> bool:
        """检查是否包含窗口函数"""
        for query in self.queries:
            if query.contains_window_function():
                return True
        return False

    def contains_aggregate_function(self) -> bool:
        """检查是否包含聚合函数"""
        for query in self.queries:
            if query.contains_aggregate_function():
                return True
        return False

    def get_referenced_columns(self) -> Set[str]:
        """获取所有引用的列"""
        columns = set()
        for query in self.queries:
            columns.update(query.get_referenced_columns())
        return columns