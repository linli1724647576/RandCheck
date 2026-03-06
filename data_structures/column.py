# Column类定义 - 存储表列信息
from dataclasses import dataclass

@dataclass
class Column:
    """表列信息"""
    name: str
    data_type: str
    category: str  # numeric, string, datetime, boolean
    is_nullable: bool
    table_name: str  # 所属表名