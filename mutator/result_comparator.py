from dataclasses import dataclass
from decimal import Decimal
import datetime as dt
import json
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from get_seedQuery import SeedQueryGenerator
from data_structures.db_dialect import get_current_dialect


@dataclass
class CompareReport:
    comparable: bool
    success: bool
    column_name_match: bool
    row_type_match: bool
    unmatched_rows_original: int
    unmatched_rows_mutated: int
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "comparable": self.comparable,
            "success": self.success,
            "column_name_match": self.column_name_match,
            "row_type_match": self.row_type_match,
            "unmatched_rows_original": self.unmatched_rows_original,
            "unmatched_rows_mutated": self.unmatched_rows_mutated,
            "error": self.error,
        }


class ResultComparator:
    def __init__(
        self,
        executor: Optional[SeedQueryGenerator] = None,
        db_config: Optional[Dict[str, Any]] = None,
    ):
        self.db_config = db_config or {}
        self.executor = executor or SeedQueryGenerator(db_config=self.db_config)

    def compare(self, original_sql: str, mutated_sql: str) -> CompareReport:
        original_rows, original_cols, original_error = self._execute_query(original_sql)
        mutated_rows, mutated_cols, mutated_error = self._execute_query(mutated_sql)

        if original_error or mutated_error:
            error_reason = original_error or mutated_error or "execution_failed"
            self._log_invalid_mutation(
                original_sql,
                mutated_sql,
                original_result=original_rows,
                mutated_result=mutated_rows,
                original_column_names=original_cols or [],
                mutated_column_names=mutated_cols or [],
                reason=error_reason,
            )
            return CompareReport(
                comparable=False,
                success=False,
                column_name_match=False,
                row_type_match=False,
                unmatched_rows_original=0,
                unmatched_rows_mutated=0,
                error=error_reason,
            )

        original_rows = list(original_rows or [])
        mutated_rows = list(mutated_rows or [])
        original_cols = list(original_cols or [])
        mutated_cols = list(mutated_cols or [])

        column_name_match = original_cols == mutated_cols
        if not column_name_match:
            self._log_invalid_mutation(
                original_sql,
                mutated_sql,
                original_result=original_rows,
                mutated_result=mutated_rows,
                original_column_names=original_cols,
                mutated_column_names=mutated_cols,
                reason="column_name_mismatch",
            )
            return CompareReport(
                comparable=True,
                success=False,
                column_name_match=False,
                row_type_match=False,
                unmatched_rows_original=len(original_rows),
                unmatched_rows_mutated=len(mutated_rows),
                error="column_name_mismatch",
            )

        forward_ok, forward_unmatched_src, forward_unmatched_tgt = self._match_rows(
            original_rows, mutated_rows
        )
        reverse_ok, reverse_unmatched_src, reverse_unmatched_tgt = self._match_rows(
            mutated_rows, original_rows
        )

        row_type_match = forward_ok or reverse_ok
        if row_type_match:
            if forward_ok and reverse_ok:
                unmatched_original = 0
                unmatched_mutated = 0
            elif forward_ok:
                unmatched_original = forward_unmatched_src
                unmatched_mutated = forward_unmatched_tgt
            else:
                unmatched_original = reverse_unmatched_tgt
                unmatched_mutated = reverse_unmatched_src

            return CompareReport(
                comparable=True,
                success=True,
                column_name_match=True,
                row_type_match=True,
                unmatched_rows_original=unmatched_original,
                unmatched_rows_mutated=unmatched_mutated,
            )

        self._log_invalid_mutation(
            original_sql,
            mutated_sql,
            original_result=original_rows,
            mutated_result=mutated_rows,
            original_column_names=original_cols,
            mutated_column_names=mutated_cols,
            reason="row_type_mismatch",
        )
        return CompareReport(
            comparable=True,
            success=False,
            column_name_match=True,
            row_type_match=False,
            unmatched_rows_original=forward_unmatched_src,
            unmatched_rows_mutated=forward_unmatched_tgt,
            error="row_type_mismatch",
        )

    def _match_rows(
        self,
        source_rows: Sequence[Any],
        target_rows: Sequence[Any],
    ) -> Tuple[bool, int, int]:
        target_counts: Dict[Tuple[str, ...], int] = {}
        for row in target_rows:
            sig = self._row_signature(row)
            target_counts[sig] = target_counts.get(sig, 0) + 1

        unmatched_source = 0
        for row in source_rows:
            sig = self._row_signature(row)
            count = target_counts.get(sig, 0)
            if count > 0:
                target_counts[sig] = count - 1
                if target_counts[sig] == 0:
                    del target_counts[sig]
            else:
                unmatched_source += 1

        unmatched_target = sum(target_counts.values())
        return unmatched_source == 0, unmatched_source, unmatched_target

    def _row_signature(self, row: Any) -> Tuple[str, ...]:
        if isinstance(row, (list, tuple)):
            values = row
        else:
            values = (row,)
        return tuple(self._type_family(value) for value in values)

    def _type_family(self, value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, (int, float, Decimal)):
            return "numeric"
        if isinstance(value, (dt.datetime, dt.date, dt.time)):
            return "datetime"
        if isinstance(value, (dict, list)):
            return "json"
        if isinstance(value, str):
            if self._is_json_text(value):
                return "json"
            return "string"
        if isinstance(value, (bytes, bytearray, memoryview)):
            return "unknown"
        return "unknown"

    def _is_json_text(self, text: str) -> bool:
        candidate = text.strip()
        if not candidate:
            return False
        if candidate[0] not in "{[":
            return False
        try:
            json.loads(candidate)
            return True
        except Exception:
            return False

    def _execute_query(self, sql: str) -> Tuple[Optional[List[Any]], Optional[List[str]], Optional[str]]:
        connection = self.executor.connect_db()
        if not connection:
            return None, None, "db_connection_failed"
        try:
            result = self.executor.execute_query_with_connection(sql, connection)
            if result is None:
                return None, None, "execution_failed"
            if isinstance(result, int):
                return None, None, "non_select_query"
            rows, cols = result
            return list(rows or []), list(cols or []), None
        except Exception as exc:
            return None, None, f"execution_error: {str(exc)}"
        finally:
            try:
                connection.close()
            except Exception:
                pass

    def _log_invalid_mutation(
        self,
        original_sql: str,
        mutated_sql: str,
        original_result: Optional[Sequence[Any]],
        mutated_result: Optional[Sequence[Any]],
        original_column_names: Sequence[str],
        mutated_column_names: Sequence[str],
        reason: str,
    ) -> None:
        try:
            dialect = get_current_dialect()
            db_type = dialect.name.upper() if dialect else "UNKNOWN"

            log_dir = f"invalid_mutation/{db_type}"
            os.makedirs(log_dir, exist_ok=True)

            class_name = self.__class__.__name__
            mutation_category = "结果比较"
            log_filename = f"{log_dir}/{class_name}_{db_type}_invalid_mutations.log"

            index_info = self._get_query_index_info(original_sql)
            original_size = "N/A" if original_result is None else len(original_result)
            mutated_size = "N/A" if mutated_result is None else len(mutated_result)

            with open(log_filename, "a", encoding="utf-8") as f:
                f.write(f"=== {mutation_category} 结果不符合预期({db_type}) ===\n")
                f.write(f"原始SQL: {original_sql}\n")
                f.write(f"变异SQL: {mutated_sql}\n")
                f.write(f"原始查询索引使用情况:\n{index_info}\n")
                f.write(f"原始结果集大小: {original_size}\n")
                f.write(f"变异结果集大小: {mutated_size}\n")
                f.write(f"原始列名: {list(original_column_names)}\n")
                f.write(f"变异列名: {list(mutated_column_names)}\n")
                f.write(f"失败原因: {reason}\n")
                f.write(f"原始结果集: {original_result}\n")
                f.write(f"变异结果集: {mutated_result}\n\n")
        except Exception:
            return

    def _get_query_index_info(self, sql_query: str) -> str:
        try:
            seed_generator = SeedQueryGenerator(db_config=self.db_config)
            connection = seed_generator.connect_db()
            if not connection:
                return "无法建立数据库连接"

            explain_sql = f"EXPLAIN {sql_query}"
            cursor = connection.cursor()
            cursor.execute(explain_sql)
            explain_results = cursor.fetchall()

            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            cursor.close()
            connection.close()

            index_info = []
            if column_names:
                for row in explain_results:
                    row_info = []
                    for i, val in enumerate(row):
                        col_name = column_names[i] if i < len(column_names) else f"col_{i}"
                        row_info.append(f"{col_name}: {val}")
                    index_info.append(" | ".join(row_info))
            else:
                for row in explain_results:
                    index_info.append(str(row))

            return "\n".join(index_info)
        except Exception as exc:
            return f"获取索引信息失败: {str(exc)}"
