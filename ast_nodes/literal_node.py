# LiteralNode类定义 - 字面量节点
from typing import Set, Any
from .ast_node import ASTNode
from data_structures.node_type import NodeType
from data_structures.db_dialect import get_dialect_config

class LiteralNode(ASTNode):
    """字面量节点（处理单引号转义）"""

    def __init__(self, value: Any, data_type: str):
        super().__init__(NodeType.LITERAL)
        self.value = value
        self.data_type = data_type
        self.category = data_type
        self.metadata = {
            'value': value,
            'data_type': data_type,
            'is_aggregate': False  # 字面量不是聚合
        }

    def to_sql(self) -> str:
        # 获取当前方言配置
        dialect = get_dialect_config()
        
        # 统一使用方言特定的字面量表示，确保类型信息被保留
        return dialect.get_literal_representation(self.value, self.data_type)

    def collect_column_aliases(self) -> Set[str]:
        """字面量不引用列别名"""
        return set()
    
    def collect_table_aliases(self) -> Set[str]:
        """字面量不引用表别名"""
        return set()