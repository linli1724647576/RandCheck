# Table类定义 - 存储表信息
from dataclasses import dataclass
from typing import List, Dict, Optional
import random
from .column import Column

@dataclass
class Table:
    """表信息"""
    name: str
    columns: List[Column]
    primary_key: str
    foreign_keys: List[Dict]  # 外键信息: {column, ref_table, ref_column}
    indexes: List[Dict] = None  # 索引信息: {name, columns, is_primary=False}

    def has_column(self, column_name: str) -> bool:
        """检查表中是否包含指定列"""
        return any(col.name == column_name for col in self.columns)

    def get_column(self, column_name: str) -> Optional[Column]:
        """获取指定列，如果不存在返回None"""
        for col in self.columns:
            if col.name == column_name:
                return col
        return None

    def get_similar_columns(self, column_name: str) -> List[Column]:
        """获取与指定列名相似的列（用于替换）"""
        if len(column_name) >= 3:
            prefix = column_name[:3]
            return [col for col in self.columns if col.name.startswith(prefix)]
        return []

    def get_random_column(self, category: Optional[str] = None) -> Column:
        """获取随机列，支持按类别筛选"""
        candidates = self.columns
        if category:
            candidates = [col for col in candidates if col.category == category]
        if not candidates:
            candidates = self.columns
        return random.choice(candidates)
    
    def get_all_indexes(self) -> List[Dict]:
        """获取所有索引（包括主键索引）"""
        if self.indexes is None:
            return []
        return self.indexes
    
    def get_non_primary_indexes(self) -> List[Dict]:
        """获取所有非主键索引"""
        if self.indexes is None:
            return []
        return [idx for idx in self.indexes if not idx.get('is_primary', False)]
    
    def add_index(self, index_name: str, columns: List[str], is_primary: bool = False) -> None:
        """添加索引信息"""
        if self.indexes is None:
            self.indexes = []
        self.indexes.append({
            'name': index_name,
            'columns': columns,
            'is_primary': is_primary
        })
    
    def has_index(self, index_name: str) -> bool:
        """检查是否存在指定名称的索引"""
        if self.indexes is None:
            return False
        return any(idx['name'] == index_name for idx in self.indexes)