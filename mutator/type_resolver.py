import re
from typing import Any, Dict, Optional

import sqlglot
from sqlglot import expressions as exp

from generate_random_sql import get_tables
from .slot_model import DataTypeFamily, SlotModel


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$")


def _normalize_func_key(name: str) -> str:
    return name.replace("_", "").upper()


class TypeResolver:
    """
    M2 implementation:
    - Populate data_type_family, raw_type, nullable using table metadata and node shape.
    - Reference logic from utils/parameter_type_checker.py but keep it lightweight/noisy-free.
    """

    def __init__(self, tables: Optional[list] = None):
        self.tables = tables or get_tables()
        self.alias_map: Dict[str, str] = {}
        self.derived_table_map: Dict[str, Dict[str, exp.Expression]] = {}
        self.function_return_map = self._build_function_return_map()
        self.function_param_map = self._build_function_param_map()

    def build_alias_map(self, ast: exp.Expression) -> None:
        self.alias_map.clear()
        self.derived_table_map.clear()
        if ast is None:
            return
        self._collect_aliases(ast)
        self._collect_derived_tables(ast)

    def resolve_slots(self, slots: list[SlotModel], ast: exp.Expression) -> list[SlotModel]:
        self.build_alias_map(ast)
        for slot in slots:
            self._resolve_slot(slot)
        return slots

    def _collect_aliases(self, node: exp.Expression) -> None:
        if node is None:
            return

        if isinstance(node, exp.Alias):
            if hasattr(node, "this") and hasattr(node, "alias"):
                alias = str(node.alias).lower()
                original_expr = str(node.this)
                self.alias_map[alias] = original_expr
        elif isinstance(node, exp.Table):
            if hasattr(node, "alias") and node.alias:
                alias = str(node.alias).lower()
                original_expr = str(node.this)
                self.alias_map[alias] = original_expr

        if hasattr(node, "args"):
            for child in node.args.values():
                if isinstance(child, (list, tuple)):
                    for item in child:
                        if isinstance(item, exp.Expression):
                            self._collect_aliases(item)
                elif isinstance(child, exp.Expression):
                    self._collect_aliases(child)

    def _collect_derived_tables(self, node: exp.Expression) -> None:
        if node is None:
            return

        if isinstance(node, exp.CTE):
            alias = getattr(node, "alias", None)
            if alias:
                alias_name = str(alias).lower()
                select_node = None
                if isinstance(node.this, exp.Select):
                    select_node = node.this
                elif isinstance(node.this, exp.Expression):
                    select_node = node.this.find(exp.Select)

                if select_node:
                    col_map: Dict[str, exp.Expression] = {}
                    for expr in select_node.expressions:
                        output_name = None
                        output_expr = expr
                        if isinstance(expr, exp.Alias):
                            output_name = str(expr.alias)
                            output_expr = expr.this
                        elif isinstance(expr, exp.Column):
                            output_name = expr.name
                        if output_name:
                            col_map[output_name.lower()] = output_expr
                    if col_map:
                        self.derived_table_map[alias_name] = col_map

        if isinstance(node, exp.Subquery):
            alias = getattr(node, "alias", None)
            if alias:
                alias_name = str(alias).lower()
                select_node = None
                if isinstance(node.this, exp.Select):
                    select_node = node.this
                elif isinstance(node.this, exp.Expression):
                    select_node = node.this.find(exp.Select)

                if select_node:
                    col_map: Dict[str, exp.Expression] = {}
                    for expr in select_node.expressions:
                        output_name = None
                        output_expr = expr
                        if isinstance(expr, exp.Alias):
                            output_name = str(expr.alias)
                            output_expr = expr.this
                        elif isinstance(expr, exp.Column):
                            output_name = expr.name
                        if output_name:
                            col_map[output_name.lower()] = output_expr
                    if col_map:
                        self.derived_table_map[alias_name] = col_map

        if hasattr(node, "args"):
            for child in node.args.values():
                if isinstance(child, (list, tuple)):
                    for item in child:
                        if isinstance(item, exp.Expression):
                            self._collect_derived_tables(item)
                elif isinstance(child, exp.Expression):
                    self._collect_derived_tables(child)

    def _resolve_slot(self, slot: SlotModel) -> None:
        node = slot.node_ptr

        if slot.slot_type in {"function_arg", "aggregate_arg"}:
            if self._apply_function_arg_constraints(slot, node):
                return

        if isinstance(node, exp.Subquery):
            self._resolve_scalar_subquery(slot, node)
            return

        if isinstance(node, exp.Cast) or isinstance(node, exp.TryCast):
            self._resolve_cast(slot, node)
            return

        if isinstance(node, exp.TsOrDsToDate):
            slot.data_type_family = "datetime"
            slot.raw_type = "DATE"
            return

        if isinstance(node, exp.Column):
            self._resolve_column(slot, node)
            return

        if isinstance(node, exp.Literal):
            self._resolve_literal(slot, node)
            return

        if isinstance(node, exp.Null):
            slot.data_type_family = "null"
            slot.raw_type = "NULL"
            slot.nullable = "true"
            return

        if isinstance(node, exp.HexString):
            slot.data_type_family = "binary"
            slot.raw_type = "BINARY"
            return

        if isinstance(node, exp.Boolean):
            slot.data_type_family = "boolean"
            slot.raw_type = "BOOLEAN"
            return

        if isinstance(node, (exp.And, exp.Or, exp.Not)):
            slot.data_type_family = "boolean"
            slot.raw_type = "BOOLEAN"
            return

        if isinstance(node, exp.Predicate):
            slot.data_type_family = "boolean"
            slot.raw_type = None
            return

        if isinstance(node, exp.Binary):
            self._resolve_binary(slot, node)
            return

        if isinstance(node, (exp.Func, exp.Anonymous)):
            self._resolve_function(slot, node)
            return

        # Default
        slot.data_type_family = "unknown"

    def _resolve_scalar_subquery(self, slot: SlotModel, node: exp.Subquery) -> None:
        """
        Infer scalar subquery type by looking at its first select expression.
        """
        select_node = None
        if isinstance(node.this, exp.Select):
            select_node = node.this
        elif isinstance(node.this, exp.Expression):
            select_node = node.this.find(exp.Select)

        if not select_node or not getattr(select_node, "expressions", None):
            slot.data_type_family = "unknown"
            return

        expr = select_node.expressions[0]
        if isinstance(expr, exp.Alias):
            expr = expr.this

        tmp = SlotModel(
            slot_id=slot.slot_id,
            clause=slot.clause,
            node_ptr=expr,
            node_class=expr.__class__.__name__,
            slot_type=slot.slot_type,
        )
        self._resolve_slot(tmp)
        slot.data_type_family = tmp.data_type_family
        slot.raw_type = tmp.raw_type
        slot.nullable = tmp.nullable

    def _resolve_column(self, slot: SlotModel, node: exp.Column) -> None:
        # Try derived table first (subquery alias columns)
        column_info = self._get_derived_column_info(str(node))
        if column_info is None:
            column_info = self._get_column_info(str(node))
        raw_type = column_info.get("type")
        slot.raw_type = raw_type
        if raw_type is None:
            slot.nullable = "unknown"
        else:
            slot.nullable = "true" if column_info.get("has_nulls") else "false"

        family = self._family_from_raw_type(raw_type)
        if family == "unknown":
            if column_info.get("is_numeric") is True:
                family = "numeric"
        slot.data_type_family = family

    def _resolve_literal(self, slot: SlotModel, node: exp.Literal) -> None:
        if getattr(node, "is_string", False):
            value = str(node.this)
            if _DATETIME_RE.match(value):
                slot.data_type_family = "datetime"
                slot.raw_type = "DATETIME"
            elif _DATE_RE.match(value):
                slot.data_type_family = "datetime"
                slot.raw_type = "DATE"
            else:
                slot.data_type_family = "string"
                slot.raw_type = "VARCHAR"
            return

        value = str(node.this)
        if "." in value:
            slot.data_type_family = "numeric"
            slot.raw_type = "FLOAT"
        else:
            slot.data_type_family = "numeric"
            slot.raw_type = "INT"

    def _resolve_binary(self, slot: SlotModel, node: exp.Binary) -> None:
        arithmetic = {
            "Add",
            "Sub",
            "Mul",
            "Div",
            "Mod",
            "Pow",
            "BitwiseAnd",
            "BitwiseOr",
            "BitwiseXor",
            "BitwiseLeftShift",
            "BitwiseRightShift",
        }
        if node.__class__.__name__ in arithmetic:
            # Try to infer datetime arithmetic: DATETIME +/- INTERVAL -> DATETIME
            def _family(expr: exp.Expression) -> Optional[str]:
                tmp = SlotModel(
                    slot_id=0,
                    clause="SELECT",
                    node_ptr=expr,
                    node_class=expr.__class__.__name__,
                    slot_type="function_arg",
                )
                self._resolve_slot(tmp)
                return tmp.data_type_family

            if node.__class__.__name__ in {"Add", "Sub"}:
                left_family = _family(getattr(node, "this", None)) if hasattr(node, "this") else None
                right_family = _family(getattr(node, "expression", None)) if hasattr(node, "expression") else None
                if left_family == "datetime" and right_family in {"datetime", "numeric", "unknown", None}:
                    slot.data_type_family = "datetime"
                    slot.raw_type = "DATETIME"
                    return
            slot.data_type_family = "numeric"
            return
        slot.data_type_family = "unknown"

    def _resolve_function(self, slot: SlotModel, node: exp.Expression) -> None:
        func_name = self._get_function_name(node)

        mapped = self._get_function_return_from_samples(func_name)
        if mapped:
            family, raw_type = mapped
        else:
            family, raw_type = self._get_function_return(func_name)

        if family == "unknown" and raw_type is None:
            inferred = self._infer_from_first_arg(node)
            if inferred:
                family, raw_type = inferred
        slot.data_type_family = family  # type: ignore[assignment]
        if raw_type:
            slot.raw_type = raw_type

    def _resolve_cast(self, slot: SlotModel, node: exp.Expression) -> None:
        to_type = node.args.get("to")
        raw_type = None
        if isinstance(to_type, exp.DataType):
            raw_type = str(to_type)
        elif to_type is not None:
            raw_type = str(to_type)

        slot.raw_type = raw_type
        slot.data_type_family = self._family_from_raw_type(raw_type)

    def _get_function_return_from_samples(self, func_name: str) -> Optional[tuple[str, Optional[str]]]:
        key = _normalize_func_key(func_name)
        return self.function_return_map.get(key)

    def _build_function_return_map(self) -> Dict[str, tuple[str, Optional[str]]]:
        try:
            from generate_random_sql import create_sample_functions
        except Exception:
            return {}

        mapping: Dict[str, tuple[str, Optional[str]]] = {}
        for func in create_sample_functions():
            key = _normalize_func_key(func.name)
            family, raw_type = self._map_return_type(func.return_type, func.name)
            mapping[key] = (family, raw_type)
        return mapping

    def _build_function_param_map(self) -> Dict[str, list[str]]:
        try:
            from generate_random_sql import create_sample_functions
        except Exception:
            return {}

        mapping: Dict[str, list[str]] = {}
        for func in create_sample_functions():
            key = _normalize_func_key(func.name)
            mapping[key] = list(func.param_types)
        return mapping

    @staticmethod
    def _map_return_type(return_type: str, func_name: str) -> tuple[str, Optional[str]]:
        rt = return_type.lower()
        if rt == "any":
            return ("unknown", None)
        if rt in {"numeric", "double", "int"}:
            raw = None
            if rt == "double":
                raw = "DOUBLE"
            elif rt == "int":
                raw = "INT"
            return ("numeric", raw)
        if rt in {"string"}:
            return ("string", "VARCHAR")
        if rt in {"date"}:
            return ("datetime", "DATE")
        if rt in {"time"}:
            return ("datetime", "TIME")
        if rt in {"datetime"}:
            return ("datetime", "DATETIME")
        if rt in {"json"}:
            return ("json", "JSON")
        if rt in {"binary"}:
            return ("spatial", "BINARY")
        if rt in {"boolean", "bool"}:
            return ("boolean", "BOOLEAN")
        return ("unknown", None)

    def _infer_from_first_arg(self, node: exp.Expression) -> Optional[tuple[str, Optional[str]]]:
        if not hasattr(node, "args") or not node.args:
            return None
        first_arg = None
        for value in node.args.values():
            if isinstance(value, list) and value:
                first_arg = value[0]
                break
            if isinstance(value, exp.Expression):
                first_arg = value
                break
        if first_arg is None or not isinstance(first_arg, exp.Expression):
            return None

        tmp = SlotModel(
            slot_id=0,
            clause="SELECT",
            node_ptr=first_arg,
            node_class=first_arg.__class__.__name__,
            slot_type="function_arg",
        )
        self._resolve_slot(tmp)
        return (tmp.data_type_family, tmp.raw_type)

    def _apply_function_arg_constraints(self, slot: SlotModel, node: exp.Expression) -> bool:
        parent_func, arg_index = self._get_parent_function_and_arg_index(node)
        if parent_func is None or arg_index is None:
            return False

        func_name = self._get_function_name(parent_func)
        expected = self._get_expected_param_type(func_name, arg_index)
        if expected is None:
            return False

        family, raw_type, constraints = self._map_param_type(func_name, expected)
        if family:
            slot.data_type_family = family
        if raw_type:
            slot.raw_type = raw_type
        if constraints:
            slot.constraints.update(constraints)
        return True

    def _get_expected_param_type(self, func_name: str, arg_index: int) -> Optional[str]:
        key = _normalize_func_key(func_name)
        param_types = self.function_param_map.get(key)
        if not param_types:
            return None
        if arg_index < 0 or arg_index >= len(param_types):
            return None
        return param_types[arg_index]

    def _map_param_type(
        self,
        func_name: str,
        expected_type: str,
    ) -> tuple[Optional[DataTypeFamily], Optional[str], Dict[str, Any]]:
        expected = expected_type.lower()
        constraints: Dict[str, Any] = {"expected_param_type": expected}

        if func_name.upper().startswith("ST_") and expected in {"string", "binary", "json"} and self._is_spatial_constructor(func_name):
            constraints["spatial_function"] = func_name
            if expected == "binary" or "WKB" in func_name.upper():
                constraints["spatial_input"] = "wkb"
            else:
                try:
                    from sql_generation.random_sql.geometry import (
                        is_geojson_function,
                        is_geohash_function,
                        is_wkt_function,
                    )
                except Exception:
                    is_geojson_function = lambda _: False  # type: ignore[assignment]
                    is_geohash_function = lambda _: False  # type: ignore[assignment]
                    is_wkt_function = lambda _: False  # type: ignore[assignment]

                if is_geojson_function(func_name):
                    constraints["spatial_input"] = "geojson"
                elif is_geohash_function(func_name):
                    constraints["spatial_input"] = "geohash"
                elif is_wkt_function(func_name):
                    constraints["spatial_input"] = "wkt"
                else:
                    constraints["spatial_input"] = "spatial_text"
            return ("spatial", "GEOMETRY", constraints)

        if expected == "numeric":
            return ("numeric", None, constraints)
        if expected == "string":
            return ("string", "VARCHAR", constraints)
        if expected == "json":
            return ("json", "JSON", constraints)
        if expected == "binary":
            return ("spatial", "BINARY", constraints)
        if expected in {"date", "time", "datetime"}:
            return ("datetime", expected.upper(), constraints)
        if expected in {"bool", "boolean"}:
            return ("boolean", "BOOLEAN", constraints)
        return ("unknown", None, constraints)

    @staticmethod
    def _is_spatial_constructor(func_name: str) -> bool:
        name = func_name.upper()
        constructors = {
            "ST_GEOMFROMTEXT",
            "ST_GEOMETRYFROMTEXT",
            "ST_LINEFROMTEXT",
            "ST_POLYGONFROMTEXT",
            "ST_POINTFROMTEXT",
            "ST_MPOINTFROMTEXT",
            "ST_MULTIPOINTFROMTEXT",
            "ST_MULTILINESTRINGFROMTEXT",
            "ST_MULTIPOLYGONFROMTEXT",
            "ST_GEOMFROMWKB",
            "ST_GEOMETRYFROMWKB",
            "ST_LINEFROMWKB",
            "ST_POLYGONFROMWKB",
            "ST_POINTFROMWKB",
            "ST_MPOINTFROMWKB",
            "ST_MULTIPOINTFROMWKB",
            "ST_MULTILINESTRINGFROMWKB",
            "ST_MULTIPOLYGONFROMWKB",
            "ST_GEOMFROMGEOJSON",
            "ST_POINTFROMGEOJSON",
            "ST_POINTFROMGEOHASH",
            "ST_GEOMFROMGEOHASH",
        }
        if name in constructors:
            return True
        # generic patterns
        return "FROMTEXT" in name or "FROMWKB" in name or "FROMGEOJSON" in name or "FROMGEOHASH" in name

    @staticmethod
    def _get_function_name(node: exp.Expression) -> str:
        name = node.__class__.__name__
        upper_name = name.upper()
        if upper_name == "TIMETOSTR":
            return "DATE_FORMAT"
        if name.upper() == "ANONYMOUS":
            return str(getattr(node, "this", "")).upper()
        return upper_name

    @staticmethod
    def _is_function_node(node: exp.Expression) -> bool:
        return isinstance(node, (exp.Func, exp.Anonymous, exp.Cast, exp.TryCast))

    @classmethod
    def _get_parent_function_and_arg_index(
        cls,
        node: exp.Expression,
    ) -> tuple[Optional[exp.Expression], Optional[int]]:
        parent = getattr(node, "parent", None)
        if parent is None:
            return None, None

        func = None
        if cls._is_function_node(parent):
            func = parent
        elif isinstance(parent, exp.Distinct):
            grandparent = getattr(parent, "parent", None)
            if grandparent is not None and cls._is_function_node(grandparent):
                func = grandparent
                parent = grandparent

        if func is None:
            return None, None

        args = getattr(func, "expressions", None)
        if not args:
            if func.__class__.__name__.upper() == "TIMETOSTR":
                this_arg = func.args.get("this")
                format_arg = func.args.get("format")
                if this_arg is node:
                    return func, 0
                if format_arg is node:
                    return func, 1
                return func, None
            return None, None

        for idx, arg in enumerate(args):
            if arg is node:
                return func, idx
        return func, None

    @staticmethod
    def _get_function_return(func_name: str) -> tuple[str, Optional[str]]:
        key = _normalize_func_key(func_name)
        numeric_functions = {
            "ABS", "SQRT", "POW", "EXP", "LN", "LOG", "LOG10", "LOG2",
            "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "COT",
            "ROUND", "TRUNCATE", "CEIL", "CEILING", "FLOOR",
            "SUM", "AVG", "MAX", "MIN", "COUNT", "VARIANCE", "STDDEV",
            "MEDIAN", "SUM_DISTINCT", "COUNT_DISTINCT",
            "STDDEVPOP", "STDDEVSAMP", "STDDEVPOPULATION", "STDDEVSAMPLE",
            "VARIANCEPOP", "VARIANCESAMP", "VARPOP", "VARSAMP",
        }
        numeric_int_functions = {
            "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
            "DAYOFWEEK", "WEEK", "WEEKOFYEAR", "QUARTER",
            "DAYOFMONTH", "DAYOFYEAR", "TIMESTAMPDIFF", "DATEDIFF",
        }
        string_functions = {
            "LOWER", "UPPER", "CONCAT", "LTRIM", "RTRIM", "TRIM",
            "REPLACE", "REPEAT", "LPAD", "RPAD", "REVERSE", "LENGTH",
            "SUBSTRING", "CHAR", "ASCII", "CONCAT_WS", "FIND_IN_SET",
            "RIGHT", "LEFT",
        }
        date_functions = {"DATE", "CURDATE", "STR_TO_DATE"}
        datetime_functions = {
            "DATETIME", "TIMESTAMP", "FROM_UNIXTIME", "UNIX_TIMESTAMP",
            "NOW", "SYSDATE", "CURRENT_TIMESTAMP",
        }
        json_functions = {
            "JSON_OBJECTAGG", "JSON_ARRAYAGG", "JSON_OBJECT", "JSON_ARRAY",
            "JSON_MERGE", "JSON_MERGE_PRESERVE", "JSON_MERGE_PATCH",
            "JSON_SET", "JSON_INSERT", "JSON_REPLACE", "JSON_REMOVE",
            "JSON_EXTRACT", "JSON_UNQUOTE",
        }

        if key in {_normalize_func_key(x) for x in json_functions}:
            return ("json", "JSON")
        if key in {_normalize_func_key(x) for x in numeric_int_functions}:
            return ("numeric", "INT")
        if key in {_normalize_func_key(x) for x in numeric_functions}:
            return ("numeric", None)
        if key in {_normalize_func_key(x) for x in string_functions}:
            return ("string", "VARCHAR")
        if key in {_normalize_func_key(x) for x in datetime_functions}:
            return ("datetime", "DATETIME")
        if key in {_normalize_func_key(x) for x in date_functions}:
            return ("datetime", "DATE")
        return ("unknown", None)

    def _get_column_info(self, column_name: str) -> Dict[str, Any]:
        column_name_lower = column_name.lower()
        if "distinct" in column_name_lower:
            column_name_lower = column_name_lower.replace("distinct", "").strip()

        if "." in column_name_lower:
            table_part, column_part = column_name_lower.split(".", 1)
            if table_part in self.alias_map:
                actual_table = self.alias_map[table_part]
                column_to_check = f"{actual_table}.{column_part}"
            else:
                column_to_check = column_name_lower
        else:
            if column_name_lower in self.alias_map:
                column_to_check = self.alias_map[column_name_lower]
            else:
                column_to_check = column_name_lower

        column_info = {
            "name": column_to_check,
            "type": None,
            "has_nulls": False,
            "is_numeric": False,
        }

        target_table = None
        target_column = column_to_check
        if "." in column_to_check:
            table_part, column_part = column_to_check.split(".", 1)
            target_table = table_part
            target_column = column_part

        if self.tables:
            found = False
            if target_table:
                for table in self.tables:
                    if table.name.lower() == target_table:
                        for column in table.columns:
                            if column.name == target_column:
                                column_info["type"] = column.data_type
                                column_info["has_nulls"] = column.is_nullable
                                column_info["is_numeric"] = column.category == "numeric"
                                found = True
                                break
                        if found:
                            break
            if not found:
                for table in self.tables:
                    for column in table.columns:
                        if column.name.lower() == column_to_check or column.name.lower() == target_column:
                            column_info["type"] = column.data_type
                            column_info["has_nulls"] = column.is_nullable
                            column_info["is_numeric"] = column.category == "numeric"
                            found = True
                            break
                    if found:
                        break

        return column_info

    def _get_derived_column_info(self, column_name: str) -> Optional[Dict[str, Any]]:
        column_name_lower = column_name.lower()
        if "." not in column_name_lower:
            return None

        table_part, column_part = column_name_lower.split(".", 1)
        if table_part in self.alias_map:
            table_part = self.alias_map[table_part].lower()
        if table_part not in self.derived_table_map:
            return None

        expr_map = self.derived_table_map[table_part]
        expr = expr_map.get(column_part)
        if expr is None:
            return None

        tmp = SlotModel(
            slot_id=0,
            clause="SELECT",
            node_ptr=expr,
            node_class=expr.__class__.__name__,
            slot_type="derived_col",
        )
        self._resolve_slot(tmp)
        return {
            "name": column_name_lower,
            "type": tmp.raw_type,
            "has_nulls": tmp.nullable == "true",
            "is_numeric": tmp.data_type_family == "numeric",
        }

    @staticmethod
    def _family_from_raw_type(raw_type: Optional[str]) -> DataTypeFamily:
        if not raw_type:
            return "unknown"
        t = raw_type.upper()
        if "JSON" in t:
            return "json"
        if any(x in t for x in ("GEOMETRY", "POINT", "LINESTRING", "POLYGON")):
            return "spatial"
        if any(x in t for x in ("BLOB", "BINARY", "VARBINARY")):
            return "binary"
        if any(x in t for x in ("DATE", "TIME", "DATETIME", "TIMESTAMP")):
            return "datetime"
        if any(x in t for x in ("CHAR", "TEXT", "STRING", "VARCHAR", "SET", "ENUM")):
            return "string"
        if any(x in t for x in ("BOOL", "BOOLEAN")):
            return "boolean"
        if any(x in t for x in ("INT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL", "BIT")):
            return "numeric"
        return "unknown"
