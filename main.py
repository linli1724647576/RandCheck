from pipeline.models import RunSettings
from pipeline.runner import run


if __name__ == '__main__':

    # =====================
    # Runtime configuration
    # =====================
    run_settings = RunSettings(
        dialect_str='mysql',
        use_extension=True,
        mutator_type='slot_m4',
        run_hours=5,
        use_database_tables=False,
        db_config={
            "host": "127.0.0.1",
            "port": 13306,
            "database": "test",
            "user": "root",
            "password": "123456",
            "dialect": "MYSQL",
        },
    )

    run(run_settings)
