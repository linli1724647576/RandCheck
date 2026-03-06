from dataclasses import dataclass
from typing import Callable, List, Optional

from sqlglot import expressions as exp

from .slot_model import DataTypeFamily, SlotModel

# Dialect-aware function support
from sql_generation.random_sql.samples import create_sample_functions


@dataclass
class OperatorSpec:
    operator_id: str
    supported_slot_types: List[str]
    supported_type_families: List[DataTypeFamily]
    description: str = ""


class Operator:
    def __init__(self, spec: OperatorSpec, apply_fn: Callable[[SlotModel], Optional[object]]):
        self.spec = spec
        self.apply_fn = apply_fn

    def apply(self, slot: SlotModel) -> Optional[object]:
        return self.apply_fn(slot)


class OperatorRegistry:
    def __init__(self) -> None:
        self._operators: List[Operator] = []
        self._supported_functions = self._load_supported_functions()
        self._register_builtin()

    def _load_supported_functions(self) -> set:
        try:
            return {f.name.upper() for f in create_sample_functions()}
        except Exception:
            # Fallback to empty set; when empty we treat functions as supported to avoid over-pruning.
            return set()

    def _supports_func(self, func_name: str) -> bool:
        if not self._supported_functions:
            return True
        return func_name.upper() in self._supported_functions

    def register(self, operator: Operator) -> None:
        self._operators.append(operator)

    def get_candidates(self, slot: SlotModel) -> List[Operator]:
        candidates: List[Operator] = []
        for op in self._operators:
            if self._matches(op.spec, slot):
                candidates.append(op)
        return candidates

    def _matches(self, spec: OperatorSpec, slot: SlotModel) -> bool:
        if spec.supported_slot_types:
            if slot.slot_type == "order_key":
                if "order_key" not in spec.supported_slot_types:
                    if "*" in spec.supported_slot_types and spec.operator_id.startswith("order_"):
                        pass
                    else:
                        return False
            elif "*" not in spec.supported_slot_types and slot.slot_type not in spec.supported_slot_types:
                return False
        if spec.supported_type_families:
            if "*" not in spec.supported_type_families and slot.data_type_family not in spec.supported_type_families:
                return False
        spatial_input = slot.constraints.get("spatial_input")
        if spatial_input and spec.operator_id.startswith("spatial_"):
            if f"spatial_{spatial_input}_" not in spec.operator_id:
                return False
        return True

    def _register_builtin(self) -> None:
        def _lit_num(value: int) -> exp.Literal:
            return exp.Literal.number(value)

        def _lit_str(value: str) -> exp.Literal:
            return exp.Literal.string(value)

        def _func0(name: str) -> exp.Expression:
            return exp.Anonymous(this=name)

        def _func(name: str, *args: exp.Expression) -> exp.Expression:
            return exp.Anonymous(this=name, expressions=list(args))

        def _cast(expr: exp.Expression, type_name: str) -> exp.Expression:
            return exp.Cast(this=expr, to=exp.DataType.build(type_name))

        def _lit_hex(value: str) -> exp.HexString:
            return exp.HexString(this=value)

        numeric_types: List[DataTypeFamily] = ["numeric"]
        string_types: List[DataTypeFamily] = ["string"]
        datetime_types: List[DataTypeFamily] = ["datetime"]
        boolean_types: List[DataTypeFamily] = ["boolean"]
        json_types: List[DataTypeFamily] = ["json"]
        spatial_types: List[DataTypeFamily] = ["spatial"]
        binary_types: List[DataTypeFamily] = ["binary"]

        any_slots = ["*"]

        numeric_ops = [
            ("num_const_zero", lambda _: _lit_num(0)),
            ("num_const_one", lambda _: _lit_num(1)),
            ("num_const_42", lambda _: _lit_num(42)),
            ("num_const_pi", lambda _: _func0("PI")),
            ("num_const_rand", lambda _: _func0("RAND")),
        ]
        for op_id, op_fn in numeric_ops:
            self.register(
                Operator(
                    OperatorSpec(
                        operator_id=op_id,
                        supported_slot_types=any_slots,
                        supported_type_families=numeric_types,
                    ),
                    op_fn,
                )
            )

        order_numeric_ops = [
            ("order_num_const_zero", lambda _: exp.Add(this=_lit_num(0), expression=_lit_num(0))),
            ("order_num_const_one", lambda _: exp.Add(this=_lit_num(1), expression=_lit_num(0))),
            ("order_num_const_42", lambda _: exp.Add(this=_lit_num(42), expression=_lit_num(0))),
            ("order_num_const_pi", lambda _: exp.Add(this=_func0("PI"), expression=_lit_num(0))),
        ]
        for op_id, op_fn in order_numeric_ops:
            self.register(
                Operator(
                    OperatorSpec(
                        operator_id=op_id,
                        supported_slot_types=["order_key"],
                        supported_type_families=numeric_types,
                    ),
                    op_fn,
                )
            )

        string_ops = [
            ("str_const_empty", lambda _: _lit_str("")),
            ("str_const_fixed", lambda _: _lit_str("sample_x")),
            ("str_const_fixed2", lambda _: _lit_str("sample_y")),
            ("str_const_uuid", lambda _: _func0("UUID")),
            ("str_const_concat", lambda _: _func("CONCAT", _lit_str("a"), _lit_str("b"))),
        ]
        for op_id, op_fn in string_ops:
            self.register(
                Operator(
                    OperatorSpec(
                        operator_id=op_id,
                        supported_slot_types=any_slots,
                        supported_type_families=string_types,
                    ),
                    op_fn,
                )
            )

        datetime_ops = [
            ("dt_const_date", lambda _: _cast(_lit_str("2023-01-01"), "DATE")),
            ("dt_const_datetime", lambda _: _cast(_lit_str("2023-01-01 00:00:00"), "DATETIME")),
            ("dt_const_date2", lambda _: _cast(_lit_str("2023-12-31"), "DATE")),
            ("dt_current_date", lambda _: _func0("CURRENT_DATE")),
            ("dt_current_timestamp", lambda _: _func0("CURRENT_TIMESTAMP")),
        ]
        for op_id, op_fn in datetime_ops:
            self.register(
                Operator(
                    OperatorSpec(
                        operator_id=op_id,
                        supported_slot_types=any_slots,
                        supported_type_families=datetime_types,
                    ),
                    op_fn,
                )
            )

        boolean_ops = [
            ("bool_true", lambda _: exp.Boolean(this=True)),
            ("bool_false", lambda _: exp.Boolean(this=False)),
            ("bool_not_false", lambda _: exp.Not(this=exp.Boolean(this=False))),
            ("bool_compare_true", lambda _: exp.EQ(this=_lit_num(1), expression=_lit_num(1))),
            ("bool_compare_false", lambda _: exp.EQ(this=_lit_num(1), expression=_lit_num(0))),
        ]
        for op_id, op_fn in boolean_ops:
            self.register(
                Operator(
                    OperatorSpec(
                        operator_id=op_id,
                        supported_slot_types=any_slots,
                        supported_type_families=boolean_types,
                    ),
                    op_fn,
                )
            )

        json_ops = [
            ("json_object", lambda _: _func("JSON_OBJECT", _lit_str("k"), _lit_str("v"))),
            ("json_object2", lambda _: _func("JSON_OBJECT", _lit_str("a"), _lit_num(1))),
            ("json_object3", lambda _: _func("JSON_OBJECT", _lit_str("x"), _lit_str("y"))),
            ("json_array", lambda _: _func("JSON_ARRAY", _lit_num(1), _lit_num(2))),
            ("json_array2", lambda _: _func("JSON_ARRAY", _lit_str("a"), _lit_str("b"))),
        ]
        for op_id, op_fn in json_ops:
            self.register(
                Operator(
                    OperatorSpec(
                        operator_id=op_id,
                        supported_slot_types=any_slots,
                        supported_type_families=json_types,
                    ),
                    op_fn,
                )
            )

        spatial_ops = [
            ("spatial_wkt_point", "ST_GeomFromText", lambda _: _func("ST_GeomFromText", _lit_str("POINT(0 0)"))),
            ("spatial_wkt_linestring", "ST_GeomFromText", lambda _: _func("ST_GeomFromText", _lit_str("LINESTRING(0 0, 1 1)"))),
            ("spatial_wkt_polygon", "ST_GeomFromText", lambda _: _func("ST_GeomFromText", _lit_str("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"))),
            ("spatial_wkb_point", "ST_GeomFromWKB", lambda _: _func("ST_GeomFromWKB", exp.HexString(this="010100000000000000000000000000000000000000"))),
            ("spatial_geojson_point", "ST_GeomFromGeoJSON", lambda _: _func("ST_GeomFromGeoJSON", _lit_str('{"type":"Point","coordinates":[0,0]}'))),
            ("spatial_geohash_point", "ST_PointFromGeoHash", lambda _: _func("ST_PointFromGeoHash", _lit_str("u4pruydqqvj"), _lit_num(12))),
        ]
        for op_id, func_name, op_fn in spatial_ops:
            if self._supports_func(func_name):
                self.register(
                    Operator(
                        OperatorSpec(
                            operator_id=op_id,
                            supported_slot_types=any_slots,
                            supported_type_families=spatial_types,
                        ),
                        op_fn,
                    )
                )

        binary_ops = [
            ("bin_const_zero", lambda _: _lit_hex("00")),
            ("bin_const_one", lambda _: _lit_hex("01")),
            ("bin_const_ff", lambda _: _lit_hex("FF")),
            ("bin_const_hex", lambda _: _lit_hex("DEADBEEF")),
        ]
        for op_id, op_fn in binary_ops:
            self.register(
                Operator(
                    OperatorSpec(
                        operator_id=op_id,
                        supported_slot_types=any_slots,
                        supported_type_families=binary_types,
                    ),
                    op_fn,
                )
            )

        null_ops = [
            ("null_literal", lambda _: exp.Null()),
            ("null_if_null", lambda _: _func("IFNULL", exp.Null(), exp.Null())),
            ("null_coalesce", lambda _: _func("COALESCE", exp.Null(), exp.Null())),
        ]
        for op_id, op_fn in null_ops:
            self.register(
                Operator(
                    OperatorSpec(
                        operator_id=op_id,
                        supported_slot_types=any_slots,
                        supported_type_families=["null"],
                    ),
                    op_fn,
                )
            )

        unknown_ops = [
            ("unknown_null", lambda _: exp.Null()),
            ("unknown_cast_string", lambda _: _cast(_lit_str("1"), "CHAR")),
            ("unknown_cast_numeric", lambda _: _cast(_lit_str("1"), "DOUBLE")),
        ]
        for op_id, op_fn in unknown_ops:
            self.register(
                Operator(
                    OperatorSpec(
                        operator_id=op_id,
                        supported_slot_types=any_slots,
                        supported_type_families=["unknown"],
                    ),
                    op_fn,
                )
            )
