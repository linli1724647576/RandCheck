from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class RunSettings:
    dialect_str: str
    use_extension: bool
    mutator_type: str
    run_hours: int
    use_database_tables: bool = False
    db_config: Optional[Dict[str, Any]] = None
