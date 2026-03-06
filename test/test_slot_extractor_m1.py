from mutator.slot_driven_mutation_pipeline import SlotDrivenMutationPipeline


def test_m1_extract_slots_with_exclusion_and_sequence():
    pipeline = SlotDrivenMutationPipeline()
    sql = (
        "SELECT SUM(t.price) AS s "
        "FROM orders t "
        "WHERE t.created_at >= '2024-01-01' "
        "GROUP BY t.user_id "
        "ORDER BY t.price DESC "
        "LIMIT 10"
    )

    result = pipeline.analyze_query(sql)

    assert result.success is True
    assert result.error is None
    assert result.slots

    slot_ids = [slot.slot_id for slot in result.slots]
    assert slot_ids == list(range(1, len(slot_ids) + 1))

    excluded_slot_types = {"table_ref", "join_type", "order_direction", "limit_value", "alias_name"}
    assert all(slot.slot_type not in excluded_slot_types for slot in result.slots)
    assert all(slot.clause != "GROUP" for slot in result.slots)
    assert any(slot.slot_type == "aggregate_arg" for slot in result.slots)
    assert any(
        slot.expr_sql == "'2024-01-01'" and slot.data_type_family == "datetime"
        for slot in result.slots
    )


def test_m1_slot_id_resets_per_query():
    pipeline = SlotDrivenMutationPipeline()
    sql_list = [
        "SELECT ABS(a) FROM t WHERE a > 1",
        "SELECT LOWER(name) FROM t2 WHERE id = 2",
    ]

    results = pipeline.analyze_queries(sql_list)

    assert len(results) == 2
    assert results[0].success is True
    assert results[1].success is True
    assert results[0].slots[0].slot_id == 1
    assert results[1].slots[0].slot_id == 1
    assert any(slot.data_type_family == "numeric" for slot in results[0].slots)


def test_m2_function_return_types_regression():
    pipeline = SlotDrivenMutationPipeline()
    sql = (
        "SELECT (SELECT JSON_OBJECTAGG('sample_100', 'sample_67') AS subq_col FROM t3 AS s198) AS col_1, "
        "CAST(pse39.c14 AS CHAR(61)) AS col_2, "
        "DAYOFWEEK(pse39.c13) AS col_3 "
        "FROM t1 AS nex34 LEFT JOIN t2 AS pse39 ON (nex34.c4 < pse39.c2) "
        "UNION ALL "
        "SELECT 24 AS col_1, prl77.c9 AS col_2, prl77.c2 AS col_3 FROM t3 AS prl77"
    )

    result = pipeline.analyze_query(sql)
    assert result.success is True

    def _find(expr_substr):
        for slot in result.slots:
            if expr_substr in slot.expr_sql:
                return slot
        return None
    def _find_with_type(expr_substr, slot_type):
        for slot in result.slots:
            if expr_substr in slot.expr_sql and slot.slot_type == slot_type:
                return slot
        return None
    def _find_exact(expr_sql):
        for slot in result.slots:
            if slot.expr_sql == expr_sql:
                return slot
        return None

    json_slot = _find_with_type("JSON_OBJECTAGG", "function_call")
    assert json_slot is not None
    assert json_slot.data_type_family == "json"

    cast_slot = _find("CAST(pse39.c14 AS CHAR(61))")
    assert cast_slot is not None
    assert cast_slot.data_type_family == "string"
    assert cast_slot.raw_type == "CHAR(61)"

    dow_slot = _find("DAY_OF_WEEK")
    assert dow_slot is not None
    assert dow_slot.data_type_family == "numeric"

    date_cast_slot = _find_exact("CAST(pse39.c13 AS DATE)")
    assert date_cast_slot is not None
    assert date_cast_slot.data_type_family == "datetime"


def test_scalar_subquery_is_slot():
    pipeline = SlotDrivenMutationPipeline()
    sql = "SELECT (SELECT 1) AS x FROM t"
    result = pipeline.analyze_query(sql)
    assert result.success is True
    assert any(slot.slot_type == "scalar_subquery" for slot in result.slots)


def test_scalar_subquery_type_inference():
    pipeline = SlotDrivenMutationPipeline()
    sql = (
        "SELECT (SELECT JSON_OBJECTAGG('sample_100', 'sample_67') AS subq_col FROM t3 AS s198) AS col_1, "
        "CAST(pse39.c14 AS CHAR(61)) AS col_2, "
        "DAYOFWEEK(pse39.c13) AS col_3 "
        "FROM t1 AS nex34 LEFT JOIN t2 AS pse39 ON (nex34.c4 < pse39.c2) "
        "UNION ALL "
        "SELECT 24 AS col_1, prl77.c9 AS col_2, prl77.c2 AS col_3 FROM t3 AS prl77"
    )
    result = pipeline.analyze_query(sql)
    assert result.success is True
    subq_slot = None
    for slot in result.slots:
        if slot.slot_type == "scalar_subquery":
            subq_slot = slot
            break
    assert subq_slot is not None
    assert subq_slot.data_type_family == "json"


def test_cte_column_type_resolution():
    pipeline = SlotDrivenMutationPipeline()
    sql = (
        "WITH cte_376 AS ("
        "SELECT pow39.c16 AS col_1, TIMESTAMPDIFF(HOUR, pow39.c5, pow39.c5) AS col_2, pow39.c11 AS col_3 "
        "FROM t2 AS pow39) "
        "SELECT MAX(s340.col_2) AS max_54 FROM cte_376 AS s340"
    )
    result = pipeline.analyze_query(sql)
    assert result.success is True
    assert any(
        slot.expr_sql == "s340.col_2" and slot.data_type_family == "numeric"
        for slot in result.slots
    )


def test_logical_or_in_where_is_boolean():
    pipeline = SlotDrivenMutationPipeline()
    sql = "SELECT 1 FROM t WHERE (1 = 1) OR (2 = 2)"
    result = pipeline.analyze_query(sql)
    assert result.success is True
    assert any(
        slot.node_class == "Or" and slot.data_type_family == "boolean"
        for slot in result.slots
    )


def test_m3_m4_placeholder_pipeline():
    pipeline = SlotDrivenMutationPipeline()
    sql = "SELECT ABS(1) AS x FROM t WHERE 1 = 1"
    result = pipeline.analyze_query(sql)
    assert result.success is True

    plan = pipeline.plan_mutations(result)
    assert plan

    exec_result = pipeline.execute_mutations(result)
    assert exec_result.mutated_sql
    assert len(exec_result.results) == len(plan)

    exec_results = pipeline.execute_single_slot_mutations(result)
    assert exec_results
    assert all(len(r.results) == 1 for r in exec_results)
