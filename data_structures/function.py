# Function类定义 - 存储函数信息
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class Function:
    """函数信息"""
    name: str
    min_params: int
    max_params: int  # None表示可变参数
    param_types: List[str]  # 参数类型
    return_type: str  # 返回值类型
    func_type: str  # scalar, aggregate, window
    format_string_required: bool = False