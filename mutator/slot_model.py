from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

DataTypeFamily = Literal[
    "numeric",
    "string",
    "datetime",
    "boolean",
    "json",
    "spatial",
    "binary",
    "null",
    "unknown",
]

NullableFlag = Literal["true", "false", "unknown"]


@dataclass
class SlotModel:
    slot_id: int
    clause: str
    node_ptr: Any
    node_class: str
    slot_type: str
    data_type_family: DataTypeFamily = "unknown"
    raw_type: Optional[str] = None
    nullable: NullableFlag = "unknown"
    scope: Dict[str, Any] = field(default_factory=dict)
    expr_sql: str = ""
    constraints: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self, include_node_ptr: bool = False) -> Dict[str, Any]:
        data = {
            "slot_id": self.slot_id,
            "clause": self.clause,
            "node_class": self.node_class,
            "slot_type": self.slot_type,
            "data_type_family": self.data_type_family,
            "raw_type": self.raw_type,
            "nullable": self.nullable,
            "scope": self.scope,
            "expr_sql": self.expr_sql,
            "constraints": self.constraints,
        }
        if include_node_ptr:
            data["node_ptr"] = self.node_ptr
        return data
