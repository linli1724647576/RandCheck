import gc
import os
import time

from changeAST import PreSolve
from generate_random_sql import Generate
from get_seedQuery import SeedQueryGenerator
from data_structures.db_dialect import set_dialect

from .logger import log_message
from .models import RunSettings


def _run_internal_generation(run_settings: RunSettings, log_file_path: str) -> str:
    log_message("Start generating SQL...", log_file_path)
    generate_start = time.time()
    if run_settings.use_database_tables and not run_settings.db_config:
        raise ValueError(
            "use_database_tables=True requires db_config in RunSettings."
        )

    Generate(
        subquery_depth=2,
        total_insert_statements=40,
        num_queries=1000,
        query_type="default",
        use_database_tables=run_settings.use_database_tables,
        db_config=run_settings.db_config,
    )
    generate_end = time.time()
    log_message(
        f"SQL generation completed in {generate_end - generate_start:.2f}s.",
        log_file_path,
    )

    log_message("Start generating seed queries...", log_file_path)
    seed_start = time.time()
    seed_query_generator = SeedQueryGenerator()
    seed_query_generator.get_seedQuery()
    seed_end = time.time()
    log_message(
        f"Seed query generation completed in {seed_end - seed_start:.2f}s.",
        log_file_path,
    )
    return "./generated_sql/seedQuery.sql"


def run(run_settings: RunSettings) -> None:
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_filename = f"execution_log_{time.strftime('%Y%m%d_%H%M%S')}.txt"
    log_file_path = os.path.join(log_dir, log_filename)

    start_time = time.time()
    log_message(
        f"Program started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}",
        log_file_path,
    )

    try:
        set_dialect(run_settings.dialect_str)
        log_message(f"Dialect set to: {run_settings.dialect_str}", log_file_path)
        log_message(f"Use extension: {run_settings.use_extension}", log_file_path)

        total_seconds = run_settings.run_hours * 3600
        cycle_count = 0
        cycle_start_time = time.time()

        log_message(
            f"Begin loop execution for up to {run_settings.run_hours} hour(s).",
            log_file_path,
        )

        while time.time() - cycle_start_time < total_seconds:
            cycle_count += 1
            log_message(f"\n===== Cycle {cycle_count} start =====", log_file_path)

            try:
                seed_file_path = _run_internal_generation(run_settings, log_file_path)

                log_message("Start presolve...", log_file_path)
                presolve_start = time.time()
                presolve = PreSolve(
                    file_path=seed_file_path,
                    extension=run_settings.use_extension,
                    db_config=run_settings.db_config,
                )
                presolve.presolve(aggregate_mutation_type=run_settings.mutator_type)
                presolve_end = time.time()
                log_message(
                    f"Presolve completed in {presolve_end - presolve_start:.2f}s.",
                    log_file_path,
                )

                elapsed = time.time() - cycle_start_time
                remaining = max(total_seconds - elapsed, 0.0)
                log_message(
                    f"Cycle {cycle_count} done. elapsed={elapsed:.2f}s remaining={remaining:.2f}s",
                    log_file_path,
                )
            except Exception as exc:
                log_message(f"Cycle {cycle_count} failed: {exc}", log_file_path)
                continue
            finally:
                gc.collect()
                log_message("Garbage collection completed.")

        end_time = time.time()
        total_time = end_time - start_time
        log_message("\n===== Run Summary =====", log_file_path)
        log_message(
            f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}",
            log_file_path,
        )
        log_message(
            f"Ended at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}",
            log_file_path,
        )
        log_message(f"Total elapsed: {total_time:.2f}s", log_file_path)
        log_message(f"Completed cycles: {cycle_count}", log_file_path)
        log_message(f"Log file saved to: {os.path.abspath(log_file_path)}", log_file_path)
    except Exception as exc:
        error_time = time.time()
        log_message(f"\nProgram failed: {exc}", log_file_path)
        log_message(
            f"Failure time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(error_time))}",
            log_file_path,
        )
        log_message(f"Elapsed before failure: {error_time - start_time:.2f}s", log_file_path)
        log_message(f"Log file saved to: {os.path.abspath(log_file_path)}", log_file_path)
        raise
