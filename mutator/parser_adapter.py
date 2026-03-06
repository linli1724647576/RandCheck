from dataclasses import dataclass
from typing import Optional

import sqlglot
from sqlglot import expressions as exp

from data_structures.db_dialect import get_current_dialect


def _get_sqlglot_dialect_name() -> str:
    dialect = get_current_dialect()
    if dialect and dialect.name.upper() == "POSTGRESQL":
        return "postgres"
    return "mysql"


@dataclass
class ParseResult:
    sql: str
    ast: Optional[exp.Expression]
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.ast is not None and self.error is None


class ParserAdapter:
    def __init__(self, dialect: Optional[str] = None):
        self.dialect = dialect or _get_sqlglot_dialect_name()

    def parse(self, sql: str) -> ParseResult:
        try:
            ast = sqlglot.parse_one(sql, read=self.dialect)
            return ParseResult(sql=sql, ast=ast)
        except Exception as exc:
            return ParseResult(sql=sql, ast=None, error=str(exc))
