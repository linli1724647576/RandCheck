from dataclasses import dataclass, field
from typing import List, Optional

from sqlglot import expressions as exp

from .parser_adapter import ParseResult, ParserAdapter
from .operator_registry import OperatorRegistry
from .mutation_executor import MutationExecutionResult, MutationExecutor
from .mutation_planner import MutationPlanItem, MutationPlanner
from .slot_extractor import SlotExtractor
from .slot_model import SlotModel
from .type_resolver import TypeResolver


@dataclass
class QuerySlotExtractionResult:
    sql: str
    success: bool
    slots: List[SlotModel] = field(default_factory=list)
    error: Optional[str] = None

    def slots_as_dict(self) -> List[dict]:
        return [slot.to_dict() for slot in self.slots]


class SlotDrivenMutationPipeline:
    """
    Current milestone M1:
    - ParserAdapter
    - SlotExtractor
    """

    def __init__(
        self,
        parser: Optional[ParserAdapter] = None,
        extractor: Optional[SlotExtractor] = None,
        resolver: Optional[TypeResolver] = None,
        registry: Optional[OperatorRegistry] = None,
        planner: Optional[MutationPlanner] = None,
        executor: Optional[MutationExecutor] = None,
    ):
        self.parser = parser or ParserAdapter()
        self.extractor = extractor or SlotExtractor()
        self.resolver = resolver or TypeResolver()
        self.registry = registry or OperatorRegistry()
        self.planner = planner or MutationPlanner(self.registry)
        self.executor = executor or MutationExecutor()

    def analyze_query(self, sql: str) -> QuerySlotExtractionResult:
        parse_result: ParseResult = self.parser.parse(sql)
        if not parse_result.success:
            return QuerySlotExtractionResult(
                sql=sql,
                success=False,
                slots=[],
                error=parse_result.error,
            )
        if not self._is_query_ast(parse_result.ast):
            return QuerySlotExtractionResult(
                sql=sql,
                success=False,
                slots=[],
                error="non_select_query",
            )

        self._preprocess_for_slots(parse_result.ast)
        slots = self.extractor.extract(parse_result.ast)
        self.resolver.resolve_slots(slots, parse_result.ast)
        preprocessed_sql = parse_result.ast.sql(
            dialect=self.parser.dialect,
            normalize=False,
            normalize_functions=False,
        )
        return QuerySlotExtractionResult(sql=preprocessed_sql, success=True, slots=slots)

    @staticmethod
    def _is_query_ast(ast) -> bool:
        if ast is None:
            return False
        query_types = ("Select", "Union", "Intersect", "Except")
        return ast.__class__.__name__ in query_types

    def analyze_queries(self, sql_list: List[str]) -> List[QuerySlotExtractionResult]:
        return [self.analyze_query(sql) for sql in sql_list]

    def plan_mutations(self, result: QuerySlotExtractionResult) -> List[MutationPlanItem]:
        return self.planner.plan(result.slots)

    def _relink_plan(
        self,
        ast,
        plan: List[MutationPlanItem],
    ) -> List[MutationPlanItem]:
        self._preprocess_for_slots(ast)
        fresh_slots = self.extractor.extract(ast)
        slot_map = {slot.slot_id: slot for slot in fresh_slots}
        relinked: List[MutationPlanItem] = []
        for item in plan:
            fresh_slot = slot_map.get(item.slot.slot_id)
            if fresh_slot is None:
                continue
            relinked.append(MutationPlanItem(slot=fresh_slot, operator=item.operator))
        return relinked

    def execute_mutations(self, result: QuerySlotExtractionResult) -> MutationExecutionResult:
        if not result.success:
            raise ValueError("Cannot execute mutations on failed parse result.")
        plan = self.plan_mutations(result)
        parse_result = self.parser.parse(result.sql)
        if not parse_result.success or parse_result.ast is None:
            raise ValueError(f"Cannot execute mutations: parse failed ({parse_result.error})")
        relinked_plan = self._relink_plan(parse_result.ast, plan)
        return self.executor.execute(parse_result.ast, relinked_plan, dialect=self.parser.dialect)

    def execute_single_slot_mutations(
        self,
        result: QuerySlotExtractionResult,
        compare_hook=None,
    ) -> List[MutationExecutionResult]:
        """
        Control-variable execution: mutate exactly one slot per run.
        compare_hook signature: (base_sql: str, mutated_sql: str, slot_id: int, operator_id: str) -> None
        """
        if not result.success:
            raise ValueError("Cannot execute mutations on failed parse result.")

        base_sql = result.sql
        plans = self.planner.plan_singletons(result.slots)
        exec_results: List[MutationExecutionResult] = []

        for plan in plans:
            parse_result = self.parser.parse(base_sql)
            if not parse_result.success or parse_result.ast is None:
                raise ValueError(f"Cannot execute mutations: parse failed ({parse_result.error})")
            relinked_plan = self._relink_plan(parse_result.ast, plan)
            exec_result = self.executor.execute(parse_result.ast, relinked_plan, dialect=self.parser.dialect)
            exec_results.append(exec_result)

            if compare_hook and plan:
                item = plan[0]
                compare_hook(base_sql, exec_result.mutated_sql, item.slot.slot_id, item.operator.spec.operator_id)

        return exec_results

    def _preprocess_for_slots(self, ast) -> None:
        if ast is None:
            return
        for node in ast.walk(bfs=False):
            if not isinstance(node, exp.Expression):
                continue
            if not isinstance(node, (exp.Select, exp.SetOperation)):
                continue
            if self._is_inside_scalar_subquery(node):
                continue
            if "order" in node.args:
                node.args.pop("order", None)
            if "limit" in node.args:
                node.args.pop("limit", None)

    def _is_inside_scalar_subquery(self, node: exp.Expression) -> bool:
        current = node
        while True:
            parent = getattr(current, "parent", None)
            if parent is None:
                return False
            if isinstance(parent, exp.Subquery) and SlotExtractor._is_scalar_subquery(parent):
                return True
            current = parent
