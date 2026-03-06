# NodeType枚举定义 - AST节点类型
from enum import Enum

class NodeType(Enum):
    """AST节点类型"""
    COLUMN_REFERENCE = "column_reference"
    FUNCTION_CALL = "function_call"
    COMPARISON = "comparison"
    ARITHMETIC = "arithmetic"
    LOGICAL = "logical"
    CASE = "case"
    LITERAL = "literal"  # 仅用于必要情况，如LIMIT
    SUBQUERY = "subquery"
    SELECT = "select"
    FROM = "from"
    WHERE = "where"
    GROUP_BY = "group_by"
    HAVING = "having"
    ORDER_BY = "order_by"
    LIMIT = "limit"
    SET_OPERATION = "set_operation"
    WITH = "with"