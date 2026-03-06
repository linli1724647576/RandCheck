# ASTNode基类定义
import string
import random
from typing import List, Dict, Any, Optional, Set
from data_structures.node_type import NodeType

class ASTNode:
    """AST节点基类"""

    def __init__(self, node_type: NodeType):
        self.id = self._generate_id()
        self.node_type = node_type
        self.children: List[ASTNode] = []
        self.parent: Optional[ASTNode] = None
        self.metadata: Dict[str, Any] = {}  # 存储节点特定信息

    def _generate_id(self) -> str:
        """生成唯一节点ID"""
        return 'node_' + ''.join(random.choices(
            string.ascii_lowercase + string.digits, k=8
        ))

    def add_child(self, child: 'ASTNode') -> None:
        """添加子节点"""
        self.children.append(child)
        child.parent = self

    def get_descendants(self) -> List['ASTNode']:
        """获取所有后代节点"""
        descendants = []
        for child in self.children:
            descendants.append(child)
            descendants.extend(child.get_descendants())
        return descendants

    def to_sql(self) -> str:
        """转换为SQL字符串（由子类实现）"""
        raise NotImplementedError("Subclasses must implement to_sql()")

    def contains_window_function(self) -> bool:
        """检查节点或其子节点是否包含窗口函数"""
        if self.node_type == NodeType.FUNCTION_CALL:
            if self.metadata.get('func_type') == 'window':
                return True

        for child in self.children:
            if child.contains_window_function():
                return True

        return False

    def contains_aggregate_function(self) -> bool:
        """检查节点或其子节点是否包含聚合函数"""
        if self.node_type == NodeType.FUNCTION_CALL:
            if self.metadata.get('func_type') == 'aggregate':
                return True

        for child in self.children:
            if child.contains_aggregate_function():
                return True

        return False