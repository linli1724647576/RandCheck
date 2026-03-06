from dataclasses import dataclass
from typing import List, Optional

from sqlglot import expressions as exp

from .mutation_planner import MutationPlanItem


@dataclass
class MutationResult:
    slot_id: int
    operator_id: str
    applied: bool


@dataclass
class MutationExecutionResult:
    mutated_sql: str
    results: List[MutationResult]


class MutationExecutor:
    def __init__(self, dialect: Optional[str] = None):
        self.dialect = dialect

    def execute(
        self,
        ast: exp.Expression,
        plan: List[MutationPlanItem],
        dialect: Optional[str] = None,
    ) -> MutationExecutionResult:
        results: List[MutationResult] = []

        for item in plan:
            slot = item.slot
            op = item.operator
            new_node: Optional[object] = op.apply(slot)

            applied = False
            if new_node is not None and hasattr(slot.node_ptr, "replace"):
                try:
                    slot.node_ptr.replace(new_node)
                    applied = True
                except Exception:
                    applied = False

            results.append(
                MutationResult(
                    slot_id=slot.slot_id,
                    operator_id=op.spec.operator_id,
                    applied=applied,
                )
            )

        effective_dialect = dialect or self.dialect
        if effective_dialect:
            mutated_sql = ast.sql(
                dialect=effective_dialect,
                normalize=False,
                normalize_functions=False,
            )
        else:
            mutated_sql = ast.sql()
        return MutationExecutionResult(mutated_sql=mutated_sql, results=results)
