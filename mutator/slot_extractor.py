from __future__ import annotations

from typing import List, Optional, Set, Tuple

from sqlglot import expressions as exp

from .slot_model import SlotModel


class SlotExtractor:
    """
    M1 implementation:
    - Walk AST and collect candidate slots.
    - Assign slot_id by traversal order (1..N) per query.
    - Filter by clause + slot_type exclusion matrix.
    """

    DEFAULT_EXCLUDED: Set[Tuple[str, str]] = {
        ("FROM", "table_ref"),
        ("JOIN", "join_type"),
        ("ORDER", "order_direction"),
        ("LIMIT", "limit_value"),
        ("SELECT", "alias_name"),
        ("GROUP", "*"),
    }
    ALWAYS_EXCLUDED_SLOT_TYPES: Set[str] = {"join_type", "table_ref"}

    def __init__(self, excluded_rules: Optional[Set[Tuple[str, str]]] = None):
        self.excluded_rules = excluded_rules or self.DEFAULT_EXCLUDED
        self._aggregate_name_set = self._build_aggregate_name_set()

    def extract(self, ast: exp.Expression) -> List[SlotModel]:
        slots: List[SlotModel] = []
        slot_id = 1

        for node in ast.walk(bfs=False):
            if not isinstance(node, exp.Expression):
                continue

            clause = self._determine_clause(node)
            slot_type = self._determine_slot_type(node, clause)
            if not slot_type:
                continue

            if self._is_excluded(clause, slot_type):
                continue

            slot = SlotModel(
                slot_id=slot_id,
                clause=clause,
                node_ptr=node,
                node_class=node.__class__.__name__,
                slot_type=slot_type,
                expr_sql=node.sql(),
            )
            slots.append(slot)
            slot_id += 1

        return slots

    def _is_excluded(self, clause: str, slot_type: str) -> bool:
        if slot_type in self.ALWAYS_EXCLUDED_SLOT_TYPES:
            return True
        return (clause, slot_type) in self.excluded_rules or (clause, "*") in self.excluded_rules

    def _determine_clause(self, node: exp.Expression) -> str:
        current = node
        while getattr(current, "parent", None):
            current = current.parent
            if isinstance(current, exp.Where):
                return "WHERE"
            if isinstance(current, exp.Having):
                return "HAVING"
            if isinstance(current, exp.Group):
                return "GROUP"
            if isinstance(current, exp.Order):
                return "ORDER"
            if isinstance(current, exp.Join):
                return "JOIN"
            if isinstance(current, exp.From):
                return "FROM"
            if isinstance(current, exp.Limit):
                return "LIMIT"
            if isinstance(current, exp.Select):
                return "SELECT"
        return "OTHER"

    def _determine_slot_type(self, node: exp.Expression, clause: str) -> Optional[str]:
        parent = getattr(node, "parent", None)
        # Do not mutate EXISTS/IN(subquery) wrapper nodes themselves (but still traverse into children)
        if isinstance(node, exp.Exists):
            return None
        if isinstance(node, exp.In) and node.args.get("query") is not None:
            return None
        if isinstance(parent, (exp.Cast, exp.TryCast)) and parent.args.get("to") is node:
            return None
        if isinstance(node, exp.If) and isinstance(parent, exp.Case):
            return None
        if isinstance(node, exp.Nullif) or isinstance(parent, exp.Nullif):
            return None

        if node.__class__.__name__ == "JSONKeyValue":
            return None

        if self._is_alias_name_node(node):
            return "alias_name"

        if isinstance(node, exp.Table):
            return "table_ref"

        if isinstance(node, exp.Join):
            return "join_type"

        if isinstance(node, exp.Ordered):
            return "order_direction"

        if isinstance(node, exp.Subquery) and self._is_scalar_subquery(node):
            return "scalar_subquery"

        if clause == "LIMIT" and isinstance(node, (exp.Literal, exp.Parameter)):
            return "limit_value"

        if isinstance(node, exp.Predicate):
            return "predicate"

        if isinstance(node, exp.Binary):
            return "binary_expr"

        parent_function = self._argument_function(node)
        if parent_function is not None:
            # Exclude TIMESTAMPDIFF first argument (interval unit keyword)
            func_name = ""
            if isinstance(parent_function, exp.Anonymous):
                func_name = str(getattr(parent_function, "this", "")).replace("_", "").upper()
            else:
                func_name = parent_function.__class__.__name__.replace("_", "").upper()
            if func_name == "TIMESTAMPDIFF":
                exprs = getattr(parent_function, "expressions", []) or []
                if exprs and exprs[0] is node:
                    return None
            if self._is_aggregate_function(parent_function):
                return "aggregate_arg"
            return "function_arg"

        if self._is_aggregate_function(node):
            return None

        if self._is_function_node(node):
            return "function_call"

        if clause == "ORDER" and self._is_order_key(node):
            return "order_key"

        if clause == "GROUP":
            return "group_key"

        if isinstance(node, exp.Column):
            return "column_ref"

        if isinstance(node, exp.Literal):
            return "literal"

        return None

    @staticmethod
    def _is_function_node(node: exp.Expression) -> bool:
        return isinstance(node, (exp.Func, exp.Anonymous, exp.Cast, exp.TryCast))

    @staticmethod
    def _normalize_func_name(name: str) -> str:
        return name.replace("_", "").upper()

    def _build_aggregate_name_set(self) -> Set[str]:
        try:
            from generate_random_sql import create_sample_functions
        except Exception:
            return set()
        names: Set[str] = set()
        for func in create_sample_functions():
            if getattr(func, "func_type", "") == "aggregate":
                names.add(self._normalize_func_name(func.name))
        return names

    def _is_aggregate_function(self, node: exp.Expression) -> bool:
        if isinstance(node, exp.AggFunc):
            return True
        if isinstance(node, (exp.Func, exp.Anonymous)):
            if isinstance(node, exp.Anonymous):
                name = str(getattr(node, "this", ""))
            else:
                name = node.__class__.__name__
            return self._normalize_func_name(name) in self._aggregate_name_set
        return False

    @staticmethod
    def _is_child_arg(parent: exp.Expression, child: exp.Expression) -> bool:
        for value in parent.args.values():
            if value is child:
                return True
            if isinstance(value, list) and any(item is child for item in value):
                return True
        return False

    def _argument_function(self, node: exp.Expression) -> Optional[exp.Expression]:
        parent = getattr(node, "parent", None)
        if parent is None:
            return None

        if self._is_function_node(parent) and self._is_child_arg(parent, node):
            return parent

        # Handle SUM(DISTINCT col) path: col -> Distinct -> AggFunc
        grandparent = getattr(parent, "parent", None)
        if isinstance(parent, exp.Distinct) and grandparent is not None:
            if self._is_function_node(grandparent):
                return grandparent

        return None

    @staticmethod
    def _is_alias_name_node(node: exp.Expression) -> bool:
        parent = getattr(node, "parent", None)
        return bool(isinstance(parent, exp.Alias) and parent.args.get("alias") is node)

    @staticmethod
    def _is_order_key(node: exp.Expression) -> bool:
        parent = getattr(node, "parent", None)
        return bool(isinstance(parent, exp.Ordered) and parent.args.get("this") is node)

    @staticmethod
    def _is_scalar_subquery(node: exp.Subquery) -> bool:
        parent = getattr(node, "parent", None)
        if isinstance(parent, (exp.From, exp.Join)):
            return False
        if isinstance(parent, exp.SetOperation):
            return False
        if isinstance(parent, (exp.Exists, exp.In, exp.Any, exp.All)):
            return False
        return True
