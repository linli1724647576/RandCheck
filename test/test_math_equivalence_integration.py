import sqlglot

from changeAST import PreSolve
from data_structures.db_dialect import set_dialect
from get_seedQuery import SeedQueryGenerator
from mutator.detail_mutator.math_equivalence_mutator import MathEquivalenceMutator


def test_math_equivalence_integration(monkeypatch, tmp_path):
    set_dialect('mysql')

    seed_path = tmp_path / 'seedQuery.sql'
    seed_path.write_text("SELECT SUM(ABS(1)) AS s\n", encoding='utf-8')

    calls = []
    original_apply = MathEquivalenceMutator._apply_mutation

    def wrapped(self, strategy, func_name):
        original_sql = self.node.sql()
        mutated = original_apply(self, strategy, func_name)
        assert mutated is not None
        assert mutated.sql() != original_sql
        calls.append((func_name, strategy))
        return mutated

    def fake_execute_query(self, query):
        return ([(1,)], ['col'])

    monkeypatch.setattr(MathEquivalenceMutator, '_apply_mutation', wrapped)
    monkeypatch.setattr(SeedQueryGenerator, 'execute_query', fake_execute_query)

    presolve = PreSolve(file_path=str(seed_path), extension=True)
    presolve.presolve(max_queries=1, aggregate_mutation_type='math_equivalence')

    assert calls
